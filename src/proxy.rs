use crate::bundle;
use crate::cli::ProxyArgs;
use crate::hostspec::{self, Target};
use crate::keys;
use crate::kubectl::{self, RemoteTarget};
use crate::paths;
use crate::port_forward::PortForward;
use crate::proxy_io;
use crate::remote;
use anyhow::{bail, Context, Result};
use log::info;
use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;
use tokio::fs;
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration};

fn init_logger(level_arg: &str) {
    let mut builder = env_logger::Builder::new();
    builder.format(|buf, record| writeln!(buf, "{}", record.args()));
    builder.parse_filters(level_arg);
    let _ = builder.try_init();
}

fn log_step_done(step: &str, start: Instant) {
    info!(
        "[sshpod] {} completed in {} ms",
        step,
        start.elapsed().as_millis()
    );
}

fn cached_remote_port_path(pod_uid: &str, container: &str) -> Result<PathBuf> {
    Ok(paths::home_dir()?
        .join(".cache/sshpod/ports")
        .join(pod_uid)
        .join(container))
}

async fn read_cached_remote_port(pod_uid: &str, container: &str) -> Result<Option<u16>> {
    let path = cached_remote_port_path(pod_uid, container)?;
    let contents = match fs::read_to_string(&path).await {
        Ok(contents) => contents,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).with_context(|| format!("failed to read {}", path.display())),
    };
    let port = contents
        .trim()
        .parse()
        .with_context(|| format!("invalid cached remote port in {}", path.display()))?;
    Ok(Some(port))
}

async fn write_cached_remote_port(pod_uid: &str, container: &str, remote_port: u16) -> Result<()> {
    let path = cached_remote_port_path(pod_uid, container)?;
    let parent = path
        .parent()
        .context("cached remote port path has no parent directory")?;
    fs::create_dir_all(parent)
        .await
        .with_context(|| format!("failed to create {}", parent.display()))?;
    fs::write(&path, format!("{remote_port}\n"))
        .await
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

async fn remove_cached_remote_port(pod_uid: &str, container: &str) -> Result<()> {
    let path = cached_remote_port_path(pod_uid, container)?;
    match fs::remove_file(&path).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("failed to remove {}", path.display())),
    }
}

async fn start_port_forward_with_retry(
    context: Option<&str>,
    namespace: &str,
    pod: &str,
    remote_port: u16,
) -> Result<(PortForward, u16)> {
    let mut last_err = None;
    for attempt in 1..=2 {
        match PortForward::start(context, namespace, pod, remote_port).await {
            Ok(forward) => return Ok(forward),
            Err(err) if attempt < 2 => {
                info!(
                    "[sshpod] port-forward attempt {} failed: {}; retrying once",
                    attempt, err
                );
                last_err = Some(err);
                sleep(Duration::from_millis(500)).await;
            }
            Err(err) => {
                last_err = Some(err);
            }
        }
    }
    Err(last_err.unwrap())
}

async fn resolve_remote_target(
    host: &hostspec::HostSpec,
) -> Result<(RemoteTarget, kubectl::PodInfo)> {
    if let Some(ctx) = &host.context {
        kubectl::ensure_context_exists(ctx).await?;
    }
    let namespace = if let Some(ns) = host.namespace.clone() {
        ns
    } else if let Some(ctx) = &host.context {
        kubectl::get_context_namespace(ctx)
            .await?
            .unwrap_or_default()
    } else {
        kubectl::get_context_namespace("default")
            .await?
            .unwrap_or_default()
    };
    let ns_str = namespace.as_str();

    let pod_name = match &host.target {
        Target::Pod(pod) => pod.clone(),
        Target::Deployment(dep) => {
            kubectl::choose_pod_for_deployment(host.context.as_deref(), ns_str, dep)
                .await
                .with_context(|| format!("failed to select pod from deployment `{}`", dep))?
        }
        Target::Job(job) => kubectl::choose_pod_for_job(host.context.as_deref(), ns_str, job)
            .await
            .with_context(|| format!("failed to select pod from job `{}`", job))?,
    };
    info!(
        "[sshpod] resolved pod: {} (namespace={}, context={})",
        pod_name,
        ns_str,
        host.context.as_deref().unwrap_or("default")
    );

    let pod_info = kubectl::get_pod_info(host.context.as_deref(), ns_str, &pod_name)
        .await
        .with_context(|| format!("failed to inspect pod {}.{}", pod_name, ns_str))?;

    let container = match host.container.as_ref() {
        Some(c) => {
            if pod_info.containers.iter().any(|name| name == c) {
                c.clone()
            } else {
                bail!("container `{}` not found in pod {}", c, pod_name);
            }
        }
        None => {
            if pod_info.containers.len() == 1 {
                pod_info.containers[0].clone()
            } else {
                bail!("This Pod has multiple containers. Use container--<container>.pod--<pod>.namespace--<namespace>[.context--<context>].sshpod to specify the target container.");
            }
        }
    };
    info!("[sshpod] resolved container: {}", container);

    let target = RemoteTarget {
        context: host.context.clone(),
        namespace,
        pod: pod_name,
        container,
    };

    Ok((target, pod_info))
}

pub async fn run(args: ProxyArgs) -> Result<()> {
    init_logger(&args.log_level);
    let total_start = Instant::now();

    let hostspec_start = Instant::now();
    let host = hostspec::parse(&args.host).context("failed to parse hostspec")?;
    log_step_done("parsed hostspec", hostspec_start);
    let login_user = args
        .user
        .filter(|u| !u.is_empty())
        .unwrap_or_else(whoami::username);

    let resolve_start = Instant::now();
    let (target, pod_info) = resolve_remote_target(&host).await?;
    log_step_done("resolved remote target", resolve_start);
    let ns_str = target.namespace.as_str();
    let pod_name = target.pod.clone();
    let container = target.container.clone();
    let pod_uid = pod_info.uid.clone();
    let base = format!("/tmp/sshpod/{}/{}", pod_info.uid, container);

    let local_key_start = Instant::now();
    let local_key = keys::ensure_key("id_ed25519")
        .await
        .context("failed to ensure ~/.cache/sshpod/id_ed25519 exists")?;
    log_step_done("ensured local client key", local_key_start);

    let cached_port_read_start = Instant::now();
    let cached_remote_port = read_cached_remote_port(&pod_uid, &container).await?;
    log_step_done("read cached remote sshd port", cached_port_read_start);

    let (mut forward, remote_port, local_port) = if let Some(cached_remote_port) = cached_remote_port
    {
        info!(
            "[sshpod] trying cached remote sshd on 127.0.0.1:{} (pod {})",
            cached_remote_port, pod_name
        );
        let port_forward_start = Instant::now();
        match start_port_forward_with_retry(
            host.context.as_deref(),
            ns_str,
            &pod_name,
            cached_remote_port,
        )
        .await
        {
            Ok((forward, local_port)) => {
                log_step_done("established port-forward", port_forward_start);
                info!(
                    "[sshpod] reusing cached remote sshd on 127.0.0.1:{} (pod {})",
                    cached_remote_port, pod_name
                );
                (forward, cached_remote_port, local_port)
            }
            Err(err) => {
                log_step_done("attempted cached port-forward", port_forward_start);
                info!(
                    "[sshpod] cached remote sshd port {} failed: {}; falling back to kubectl exec",
                    cached_remote_port, err
                );
                remove_cached_remote_port(&pod_uid, &container).await?;

                let remote_sshd_start = Instant::now();
                let remote_port = if let Some(port) = remote::existing_sshd_port(&target, &base).await? {
                    log_step_done("checked existing remote sshd", remote_sshd_start);
                    info!(
                        "[sshpod] reusing existing sshd on 127.0.0.1:{} (pod {})",
                        port, pod_name
                    );
                    port
                } else {
                    log_step_done("checked existing remote sshd", remote_sshd_start);

                    let lock_start = Instant::now();
                    remote::try_acquire_lock(&target, &base).await;
                    log_step_done("attempted remote setup lock", lock_start);

                    let user_check_start = Instant::now();
                    remote::assert_login_user_allowed(&target, &login_user).await?;
                    log_step_done("validated remote login user", user_check_start);

                    let arch_start = Instant::now();
                    let arch = bundle::detect_remote_arch(&target)
                        .await
                        .context("failed to detect remote arch")?;
                    log_step_done("detected remote architecture", arch_start);
                    info!("[sshpod] remote architecture: {}", arch);

                    let bundle_start = Instant::now();
                    bundle::ensure_bundle(&target, &base, &arch).await?;
                    log_step_done("ensured remote sshd bundle", bundle_start);
                    info!("[sshpod] sshd bundle ready for pod {}", pod_name);

                    let host_keys_start = Instant::now();
                    let host_keys = keys::ensure_key("ssh_host_ed25519_key")
                        .await
                        .context("failed to create host keys")?;
                    log_step_done("ensured local host keypair", host_keys_start);

                    let install_host_keys_start = Instant::now();
                    remote::install_host_keys(&target, &base, &host_keys).await?;
                    log_step_done("installed remote host keys", install_host_keys_start);

                    info!("[sshpod] starting/ensuring sshd in pod {}", pod_name);
                    let ensure_sshd_start = Instant::now();
                    let port = remote::ensure_sshd_running(
                        &target,
                        &base,
                        &login_user,
                        &local_key.public,
                    )
                    .await?;
                    log_step_done("started remote sshd", ensure_sshd_start);
                    info!(
                        "[sshpod] sshd is listening on 127.0.0.1:{} (pod {})",
                        port, pod_name
                    );
                    port
                };

                write_cached_remote_port(&pod_uid, &container, remote_port).await?;
                info!(
                    "[sshpod] starting port-forward to {}:{}",
                    pod_name, remote_port
                );
                let port_forward_start = Instant::now();
                let (forward, local_port) = start_port_forward_with_retry(
                    host.context.as_deref(),
                    ns_str,
                    &pod_name,
                    remote_port,
                )
                .await?;
                log_step_done("established port-forward", port_forward_start);
                (forward, remote_port, local_port)
            }
        }
    } else {
        let remote_sshd_start = Instant::now();
        let remote_port = if let Some(port) = remote::existing_sshd_port(&target, &base).await? {
            log_step_done("checked existing remote sshd", remote_sshd_start);
            info!(
                "[sshpod] reusing existing sshd on 127.0.0.1:{} (pod {})",
                port, pod_name
            );
            port
        } else {
            log_step_done("checked existing remote sshd", remote_sshd_start);

            let lock_start = Instant::now();
            remote::try_acquire_lock(&target, &base).await;
            log_step_done("attempted remote setup lock", lock_start);

            let user_check_start = Instant::now();
            remote::assert_login_user_allowed(&target, &login_user).await?;
            log_step_done("validated remote login user", user_check_start);

            let arch_start = Instant::now();
            let arch = bundle::detect_remote_arch(&target)
                .await
                .context("failed to detect remote arch")?;
            log_step_done("detected remote architecture", arch_start);
            info!("[sshpod] remote architecture: {}", arch);

            let bundle_start = Instant::now();
            bundle::ensure_bundle(&target, &base, &arch).await?;
            log_step_done("ensured remote sshd bundle", bundle_start);
            info!("[sshpod] sshd bundle ready for pod {}", pod_name);

            let host_keys_start = Instant::now();
            let host_keys = keys::ensure_key("ssh_host_ed25519_key")
                .await
                .context("failed to create host keys")?;
            log_step_done("ensured local host keypair", host_keys_start);

            let install_host_keys_start = Instant::now();
            remote::install_host_keys(&target, &base, &host_keys).await?;
            log_step_done("installed remote host keys", install_host_keys_start);

            info!("[sshpod] starting/ensuring sshd in pod {}", pod_name);
            let ensure_sshd_start = Instant::now();
            let port =
                remote::ensure_sshd_running(&target, &base, &login_user, &local_key.public)
                    .await?;
            log_step_done("started remote sshd", ensure_sshd_start);
            info!(
                "[sshpod] sshd is listening on 127.0.0.1:{} (pod {})",
                port, pod_name
            );
            port
        };

        write_cached_remote_port(&pod_uid, &container, remote_port).await?;
        info!(
            "[sshpod] starting port-forward to {}:{}",
            pod_name, remote_port
        );
        let port_forward_start = Instant::now();
        let (forward, local_port) =
            start_port_forward_with_retry(host.context.as_deref(), ns_str, &pod_name, remote_port)
                .await?;
        log_step_done("established port-forward", port_forward_start);
        (forward, remote_port, local_port)
    };

    info!(
        "[sshpod] port-forward established: localhost:{} -> {}:{}",
        local_port, pod_name, remote_port
    );

    let tcp_connect_start = Instant::now();
    let stream = TcpStream::connect(("127.0.0.1", local_port))
        .await
        .context("failed to connect to forwarded sshd port")?;
    log_step_done("connected local tcp stream", tcp_connect_start);

    info!(
        "[sshpod] setup completed in {} ms",
        total_start.elapsed().as_millis()
    );

    let pump_result = proxy_io::pump(stream).await;
    let stop_result = forward.stop().await;

    pump_result?;
    stop_result?;
    Ok(())
}
