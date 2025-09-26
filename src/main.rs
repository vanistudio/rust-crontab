use anyhow::{Context, Result};
use clap::Parser;
use dashmap::DashMap;
use rand::Rng;
use reqwest::Client;
use std::{fs, sync::Arc, time::Duration};
use tokio::{
    sync::Semaphore,
    time::{sleep, Instant},
};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use url::Url;

#[derive(Parser, Debug)]
struct Opts {
    #[arg(long, default_value = "urls.txt")]
    file: String,
    #[arg(long, default_value_t = 512)]
    global_concurrency: usize,
    #[arg(long, default_value_t = 32)]
    per_host_concurrency: usize,
    #[arg(long, default_value_t = 5000)]
    timeout_ms: u64,
    #[arg(long, default_value_t = 2)]
    retries: usize,
    #[arg(long, default_value = "HEAD")]
    method: String,
}

#[derive(Clone)]
struct UrlEntry {
    url: String,
    interval_secs: u64,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let opts = Opts::parse();
    let content = fs::read_to_string(&opts.file)
        .with_context(|| format!("Không đọc được file {}", &opts.file))?;

    let mut entries: Vec<UrlEntry> = Vec::new();
    for (i, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parts: Vec<&str> = line.split('|').map(|s| s.trim()).collect();
        if parts.len() != 2 {
            warn!("Dòng {} bỏ qua: định dạng phải là link|seconds", i + 1);
            continue;
        }
        let link = parts[0];
        let secs = match parts[1].parse::<u64>() {
            Ok(s) if s > 0 => s,
            _ => {
                warn!("Dòng {} bỏ qua: thời gian không hợp lệ", i + 1);
                continue;
            }
        };
        if !(link.starts_with("http://") || link.starts_with("https://")) {
            warn!("Dòng {} bỏ qua: URL thiếu protocol (http/https)", i + 1);
            continue;
        }
        entries.push(UrlEntry {
            url: link.to_string(),
            interval_secs: secs,
        });
    }

    if entries.is_empty() {
        anyhow::bail!("Không có URL hợp lệ trong file {}", opts.file);
    }

    info!("Đã nạp {} URL", entries.len());

    let client = Client::builder()
        .user_agent("cron_probe_strong/0.1")
        .tcp_nodelay(true)
        .pool_idle_timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(64)
        .connect_timeout(Duration::from_secs(10))
        .http2_adaptive_window(true)
        .build()
        .context("Tạo HTTP client thất bại")?;
    let client = Arc::new(client);

    let global_sem = Arc::new(Semaphore::new(opts.global_concurrency));
    let host_semaphores: Arc<DashMap<String, Arc<Semaphore>>> = Arc::new(DashMap::new());
    let stats = Arc::new(DashMap::<String, serde_json::Value>::new());

    for entry in entries.into_iter() {
        let client = client.clone();
        let global_sem = global_sem.clone();
        let host_semaphores = host_semaphores.clone();
        let stats = stats.clone();
        let per_host_limit = opts.per_host_concurrency;
        let timeout_ms = opts.timeout_ms;
        let retries = opts.retries;
        let method = opts.method.clone();

        tokio::spawn(async move {
            let host = Url::parse(&entry.url)
                .ok()
                .and_then(|u| u.host_str().map(|s| s.to_string()))
                .unwrap_or_else(|| "unknown".into());
            let host_sem = host_semaphores
                .entry(host.clone())
                .or_insert_with(|| Arc::new(Semaphore::new(per_host_limit)))
                .value()
                .clone();
            let jitter = rand::thread_rng().gen_range(0..entry.interval_secs.min(10));
            sleep(Duration::from_secs(jitter)).await;

            loop {
                let start = Instant::now();
                let g_permit = global_sem.acquire().await.unwrap();
                let h_permit = host_sem.acquire().await.unwrap();

                let mut attempt = 0usize;
                let res_time = loop {
                    attempt += 1;
                    let op = tokio::time::timeout(Duration::from_millis(timeout_ms), async {
                        let builder = match method.as_str() {
                            "GET" => client.get(&entry.url),
                            _ => client.head(&entry.url),
                        };
                        builder.send().await
                    })
                    .await;

                    match op {
                        Ok(Ok(response)) => {
                            if response.status().is_success() {
                                break Some(start.elapsed().as_millis() as u128);
                            } else {
                                warn!(
                                    "URL {} status {} (attempt {}/{})",
                                    &entry.url,
                                    response.status(),
                                    attempt,
                                    retries
                                );
                                if attempt >= retries {
                                    break None;
                                }
                            }
                        }
                        Ok(Err(e)) => {
                            warn!(
                                "Request error {}: {} (attempt {}/{})",
                                &entry.url, e, attempt, retries
                            );
                            if attempt >= retries {
                                break None;
                            }
                        }
                        Err(_) => {
                            warn!(
                                "Timeout {} (attempt {}/{})",
                                &entry.url, attempt, retries
                            );
                            if attempt >= retries {
                                break None;
                            }
                        }
                    }
                    let backoff = rand::thread_rng().gen_range(50..=250);
                    sleep(Duration::from_millis(backoff)).await;
                };

                drop(g_permit);
                drop(h_permit);

                let mut entry_stats = stats.entry(entry.url.clone()).or_insert(serde_json::json!({
                    "checks": 0,
                    "success": 0,
                    "fail": 0,
                    "last_ms": null,
                    "last_at": null
                }));

                let mut obj = entry_stats.value().clone();
                let mut checks = obj["checks"].as_u64().unwrap_or(0);
                checks += 1;
                obj["checks"] = serde_json::json!(checks);
                if let Some(ms) = res_time {
                    let success = obj["success"].as_u64().unwrap_or(0) + 1;
                    obj["success"] = serde_json::json!(success);
                    obj["last_ms"] = serde_json::json!(ms);
                    obj["last_at"] = serde_json::json!(chrono::Utc::now().to_rfc3339());
                } else {
                    let fail = obj["fail"].as_u64().unwrap_or(0) + 1;
                    obj["fail"] = serde_json::json!(fail);
                    obj["last_ms"] = serde_json::Value::Null;
                    obj["last_at"] = serde_json::json!(chrono::Utc::now().to_rfc3339());
                }
                *entry_stats.value_mut() = obj;

                match res_time {
                    Some(ms) => info!("OK {} ({} ms) next {}s", &entry.url, ms, entry.interval_secs),
                    None => warn!("FAIL {} next {}s", &entry.url, entry.interval_secs),
                }
                sleep(Duration::from_secs(entry.interval_secs)).await;
            }
        });
    }

    tokio::signal::ctrl_c().await?;
    info!("Đã nhận Ctrl+C, in thống kê...");
    for item in stats.iter() {
        println!("URL: {}", item.key());
        println!("{}", serde_json::to_string_pretty(&item.value()).unwrap());
    }
    info!("Thoát.");
    Ok(())
}
