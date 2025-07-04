use anyhow::{Context, Result}; // 引入错误处理库
use chrono::{DateTime, Utc}; // 引入时间库
use futures::future::join_all; // 并发任务等待工具
use log::{error, info}; // 日志宏
use reqwest; // HTTP 客户端
use serde_json::Value; // JSON值类型
use sha2::{Digest, Sha256}; // sha256哈希
use std::collections::{HashMap, HashSet}; // 哈希表/集合
use std::fs::File; // 文件操作
use std::fs::OpenOptions;
use std::io::{self, Write}; // 文件写入
use structopt::StructOpt; // 命令行参数解析
use std::time::Duration; // 用于设置超时的Duration类型
use std::sync::Arc; // 新增：用于 Client 复用

#[derive(StructOpt, Debug)]
#[structopt(
    name = "datacp",
    about = "ClickHouse数据迁移工具")]
struct Opt {
    /// 源ClickHouse DSN (仅支持http)
    #[structopt(long, default_value = "http://default:@localhost:8123")]
    src_dsn: String, // 源库连接串
    /// 目标ClickHouse DSN (仅支持http)
    #[structopt(long, default_value = "http://default:@localhost:8123")]
    dst_dsn: String, // 目标库连接串
    /// 源数据库名，必填
    #[structopt(long, default_value="db_data")]
    src_db: String, // 源数据库名
    /// 目标数据库名，必填
    #[structopt(long, default_value="db_data")]
    dst_db: String, // 目标数据库名
    /// 源表名，必填
    #[structopt(long, default_value="")]
    src_table: String, // 源表名
    /// 目标表名，必填
    #[structopt(long, default_value="")]
    dst_table: String, // 目标表名
    /// 用于迁移的时间字段（DateTime类型），必填
    #[structopt(long, default_value="")]
    time_field: String, // 时间字段
    /// 迁移起始时间，默认: 1970-01-01 08:00:01
    #[structopt(long, default_value = "1970-01-01 08:00:01")]
    start_time: String, // 起始时间
    /// 并发数，默认: 4
    #[structopt(long, default_value = "4")]
    parallelism: usize, // 并发数
    /// 断点续传文件名，留空自动生成
    #[structopt(long, default_value = "")]
    done_segments: String, // 断点续传文件名
    /// 忽略校验和插入的字段，可指定多次
    #[structopt(long = "ignore-field", use_delimiter = true)]
    ignore_field: Vec<String>, // 忽略字段
    /// 日志文件名，默认: log.json
    #[structopt(long, default_value = "log.json")]
    log_file: String, // 日志文件名
    /// 源表是否为分布式表，默认: false
    #[structopt(long, parse(try_from_str), default_value = "false")]
    is_src_distributed: bool, // 源表是否分布式
    /// 目标表是否为分布式表，默认: false
    #[structopt(long, parse(try_from_str), default_value = "false")]
    is_dst_distributed: bool, // 目标表是否分布式
    /// ClickHouse集群名（分布式表rename时用）
    #[structopt(long, default_value = "")]
    cluster_name: String, // 集群名
}

fn is_ignored_field(name: &str, ignore_fields: &[String]) -> bool {
    ignore_fields.iter().any(|f| f == name) // 判断字段名是否在忽略列表
}

// ===================== HTTP 方案主流程相关函数 =====================

// 表结构校验（HTTP 方案，支持 ignore_fields）
async fn compare_table_columns_http(
    src_dsn: &str,
    src_db: &str,
    dst_dsn: &str,
    dst_db: &str,
    src_table: &str,
    dst_table: &str,
    ignore_fields: &[String],
) -> anyhow::Result<()> {
    let src_cols = get_column_names_http(src_dsn, src_db, src_table).await?;
    let dst_cols = get_column_names_http(dst_dsn, dst_db, dst_table).await?;
    let src_cols: Vec<String> = src_cols.iter().filter(|c| !is_ignored_field(c, ignore_fields)).cloned().collect();
    let dst_cols: Vec<String> = dst_cols.iter().filter(|c| !is_ignored_field(c, ignore_fields)).cloned().collect();
    if src_cols.len() != dst_cols.len() {
        return Err(anyhow::anyhow!(format!("源表和目标表字段数量不一致(忽略字段后): 源表{} 目标表{}", src_cols.len(), dst_cols.len())));
    }
    for (s, d) in src_cols.iter().zip(dst_cols.iter()) {
        if s != d {
            return Err(anyhow::anyhow!(format!("字段不一致: 源表[{}], 目标表[{}]", s, d)));
        }
    }
    Ok(())
}

// migrate_segment_worker: 处理分段迁移、断点续传、批量写入、详细日志（HTTP 方案）
async fn migrate_segment_worker_http(
    segments: Vec<String>,
    src_dsn: String,
    dst_dsn: String,
    src_db: String,
    dst_db: String,
    src_table: String,
    dst_table: String,
    time_field: String,
    col_names: Vec<String>,
    sorted_col_names: Vec<String>,
    ignore_fields: Vec<String>,
    done_segments_file: String,
    log_file_path: String,
    client: Arc<reqwest::Client>, // 新增参数
) {
    for seg in segments {
        info!("segment {seg} start");
        let seg_end = chrono::NaiveDateTime::parse_from_str(&seg, "%Y-%m-%d %H:%M:%S").unwrap() + chrono::Duration::hours(1);
        let seg_end_str = seg_end.format("%Y-%m-%d %H:%M:%S").to_string();
        let q = format!("SELECT {} FROM {} WHERE {} >= '{}' AND {} < '{}' FORMAT JSONEachRow", col_names.join(","), src_table, time_field, seg, time_field, seg_end_str);
        info!("segment {seg} src SQL: {q}");
        let src_rows = match ch_query_rows_with_client(&src_dsn, &src_db, &q, client.clone()).await {
            Ok(b) => b,
            Err(e) => { error!("segment {seg} failed: {e}"); continue; }
        };
        let q_dst = format!("SELECT {} FROM {} WHERE {} >= '{}' AND {} < '{}' FORMAT JSONEachRow", col_names.join(","), dst_table, time_field, seg, time_field, seg_end_str);
        info!("segment {seg} dst SQL: {q_dst}");
        let dst_rows = match ch_query_rows_with_client(&dst_dsn, &dst_db, &q_dst, client.clone()).await {
            Ok(b) => b,
            Err(e) => { error!("segment {seg} dst failed: {e}"); continue; }
        };
        let dst_row_set: HashSet<String> = dst_rows.iter().map(|r| {
            let mut norm = serde_json::Map::new();
            for col in &sorted_col_names {
                let v = r.get(col).cloned().unwrap_or(Value::Null);
                norm.insert(col.clone(), v);
            }
            let b = serde_json::to_vec(&norm).unwrap();
            let mut hasher = Sha256::new();
            hasher.update(&b);
            format!("{:x}", hasher.finalize())
        }).collect();
        let mut need_insert = Vec::new();
        for row in src_rows.iter() {
            let mut norm = serde_json::Map::new();
            for col in &sorted_col_names {
                let v = row.get(col).cloned().unwrap_or(Value::Null);
                norm.insert(col.clone(), v);
            }
            let b = serde_json::to_vec(&norm).unwrap();
            let mut hasher = Sha256::new();
            hasher.update(&b);
            let key = format!("{:x}", hasher.finalize());
            if !dst_row_set.contains(&key) {
                need_insert.push(row.clone());
            }
        }
        let mut rows_written = 0;
        if !need_insert.is_empty() {
            for batch in need_insert.chunks(5000) { // 优化：批量写入粒度提升
                let json_rows: Vec<String> = batch.iter().map(|row| serde_json::to_string(row).unwrap()).collect();
                let data = json_rows.join("\n");
                if let Err(e) = insert_rows_http_with_client(&dst_dsn, &dst_db, &dst_table, data, client.clone()).await {
                    error!("segment {seg} batch insert failed: {e}");
                    continue;
                }
                rows_written += batch.len();
            }
        }
        info!("segment {seg} end, src_rows={}, inserted={}", src_rows.len(), rows_written);
        if let Err(e) = save_done_segment(&done_segments_file, &seg) {
            error!("save_done_segment failed: {e}");
        }
    }
}

// 新增：全局复用 Client 的 HTTP 查询
async fn ch_query_rows_with_client(
    dsn: &str,
    db: &str,
    sql: &str,
    client: Arc<reqwest::Client>,
) -> anyhow::Result<Vec<HashMap<String, Value>>> {
    let (url, user, pass, _) = parse_clickhouse_dsn(dsn, db)?;
    let mut last_err = None;
    for _ in 0..3 {
        match client
            .post(&url)
            .basic_auth(&user, Some(&pass))
            .body(sql.to_string())
            .send()
            .await
        {
            Ok(resp) => {
                let status = resp.status();
                let text = resp.text().await?;
                if !status.is_success() {
                    last_err = Some(anyhow::anyhow!(format!("ClickHouse HTTP 错误: {} {}", status, text)));
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
                let mut rows = Vec::new();
                for line in text.lines() {
                    if line.trim().is_empty() { continue; }
                    let v: HashMap<String, Value> = serde_json::from_str(line)?;
                    rows.push(v);
                }
                return Ok(rows);
            }
            Err(e) => {
                last_err = Some(anyhow::anyhow!(format!("ClickHouse HTTP 连接失败: {}", e)));
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("ClickHouse HTTP 连接失败: 未知错误")))
}

// 新增：全局复用 Client 的批量写入
async fn insert_rows_http_with_client(
    dsn: &str,
    db: &str,
    table: &str,
    data: String,
    client: Arc<reqwest::Client>,
) -> anyhow::Result<()> {
    let (url, user, pass, _) = parse_clickhouse_dsn(dsn, db)?;
    let sql = format!("INSERT INTO {} FORMAT JSONEachRow", table);
    let mut last_err = None;
    for _ in 0..3 {
        match client
            .post(&url)
            .basic_auth(&user, Some(&pass))
            .query(&[("query", sql.clone())])
            .body(data.clone())
            .send()
            .await
        {
            Ok(resp) => {
                let status = resp.status();
                let text = resp.text().await?;
                if !status.is_success() {
                    last_err = Some(anyhow::anyhow!(format!("ClickHouse 批量写入失败: {} {}", status, text)));
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
                return Ok(());
            }
            Err(e) => {
                last_err = Some(anyhow::anyhow!(format!("ClickHouse HTTP 连接失败: {}", e)));
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("ClickHouse HTTP 连接失败: 未知错误")))
}

// ===================== ClickHouse HTTP 认证最小化测试 =====================
async fn test_reqwest_clickhouse_auth(dsn: &str) -> anyhow::Result<()> {
    // 只支持 http(s)://user:pass@host:port 形式
    let url = if dsn.starts_with("http://") || dsn.starts_with("https://") {
        let mut url: String = dsn.to_string();
        // 去掉末尾 /db_data
        if let Some(idx) = url.rfind('/') {
            let after = &url[idx+1..];
            if !after.contains(":") && !after.contains("@") && !after.contains("?") && !after.contains("=") {
                url = url[..idx].to_string();
            }
        }
        url
    } else {
        anyhow::bail!("只支持 http(s)://user:pass@host:port 形式");
    };
    // 解析用户名密码
    let re = regex::Regex::new(r"https?://([^:]+):([^@]+)@([^/]+)").unwrap();
    let caps = re.captures(&url).ok_or_else(|| anyhow::anyhow!(format!("DSN 格式不正确: {}", url)))?;
    let user = &caps[1];
    let pass = &caps[2];
    let host = &caps[3];
    let url = format!("http://{}/", host); // 直接访问根路径
    let sql = "SELECT 1";
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .basic_auth(user, Some(pass))
        .body(sql)
        .send()
        .await?;
    let status = resp.status();
    let text = resp.text().await?;
    println!("[reqwest] HTTP status: {status}, body: {text}");
    if !status.is_success() {
        anyhow::bail!(format!("reqwest 认证失败: {status} {text}"));
    }
    Ok(())
}

// ===================== ClickHouse HTTP 方案 =====================
// 解析 DSN，返回 (url, user, pass, db)
fn parse_clickhouse_dsn(dsn: &str, db: &str) -> anyhow::Result<(String, String, String, String)> {
    let re = regex::Regex::new(r"https?://([^:]+):([^@]*)@([^/:]+)(?::(\\d+))?/?").unwrap();
    let caps = re.captures(dsn).ok_or_else(|| anyhow::anyhow!(format!("DSN 格式不正确: {}", dsn)))?;
    let user = &caps[1];
    let pass = &caps[2];
    let host = &caps[3];
    let port = caps.get(4).map(|m| m.as_str()).unwrap_or("8123");
    let url = format!("http://{}:{}/?database={}", host, port, db);
    Ok((url, user.to_string(), pass.to_string(), db.to_string()))
}

// HTTP 查询，返回 Vec<HashMap<String, Value>>
async fn ch_query_rows(
    dsn: &str,
    db: &str,
    sql: &str,
) -> anyhow::Result<Vec<HashMap<String, Value>>> {
    let (url, user, pass, _) = parse_clickhouse_dsn(dsn, db)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let mut last_err = None;
    for _ in 0..3 {
        match client
            .post(&url)
            .basic_auth(&user, Some(&pass))
            .body(sql.to_string())
            .send()
            .await
        {
            Ok(resp) => {
                let status = resp.status();
                let text = resp.text().await?;
                if !status.is_success() {
                    last_err = Some(anyhow::anyhow!(format!("ClickHouse HTTP 错误: {} {}", status, text)));
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
                let mut rows = Vec::new();
                for line in text.lines() {
                    if line.trim().is_empty() { continue; }
                    let v: HashMap<String, Value> = serde_json::from_str(line)?;
                    rows.push(v);
                }
                return Ok(rows);
            }
            Err(e) => {
                last_err = Some(anyhow::anyhow!(format!("ClickHouse HTTP 连接失败: {}", e)));
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("ClickHouse HTTP 连接失败: 未知错误")))
}

// HTTP 执行无返回 SQL，带超时和重试
async fn ch_execute(
    dsn: &str,
    db: &str,
    sql: &str,
) -> anyhow::Result<()> {
    let (url, user, pass, _) = parse_clickhouse_dsn(dsn, db)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let mut last_err = None;
    for _ in 0..3 {
        match client
            .post(&url)
            .basic_auth(&user, Some(&pass))
            .body(sql.to_string())
            .send()
            .await
        {
            Ok(resp) => {
                let status = resp.status();
                let text = resp.text().await?;
                if !status.is_success() {
                    last_err = Some(anyhow::anyhow!(format!("ClickHouse HTTP 错误: {} {}", status, text)));
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
                return Ok(());
            }
            Err(e) => {
                last_err = Some(anyhow::anyhow!(format!("ClickHouse HTTP 连接失败: {}", e)));
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("ClickHouse HTTP 连接失败: 未知错误")))
}

// 批量写入（HTTP 方案，JSONEachRow），带超时和重试
async fn insert_rows_http(
    dsn: &str,
    db: &str,
    table: &str,
    data: String,
) -> anyhow::Result<()> {
    let (url, user, pass, _) = parse_clickhouse_dsn(dsn, db)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let sql = format!("INSERT INTO {} FORMAT JSONEachRow", table);
    let mut last_err = None;
    for _ in 0..3 {
        match client
            .post(&url)
            .basic_auth(&user, Some(&pass))
            .query(&[("query", sql.clone())])
            .body(data.clone())
            .send()
            .await
        {
            Ok(resp) => {
                let status = resp.status();
                let text = resp.text().await?;
                if !status.is_success() {
                    last_err = Some(anyhow::anyhow!(format!("ClickHouse 批量写入失败: {} {}", status, text)));
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
                return Ok(());
            }
            Err(e) => {
                last_err = Some(anyhow::anyhow!(format!("ClickHouse HTTP 连接失败: {}", e)));
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("ClickHouse HTTP 连接失败: 未知错误")))
}

// 获取所有字段名（HTTP 方案）
async fn get_column_names_http(dsn: &str, db: &str, table: &str) -> anyhow::Result<Vec<String>> {
    let sql = format!("DESCRIBE TABLE {} FORMAT JSONEachRow", table);
    let rows = ch_query_rows(dsn, db, &sql).await?;
    Ok(rows.into_iter().map(|mut r| r.remove("name").and_then(|v| v.as_str().map(|s| s.to_string())).unwrap_or_default()).collect())
}

// 获取最大时间戳（HTTP 方案）
async fn get_max_time_http(dsn: &str, db: &str, table: &str, time_field: &str) -> anyhow::Result<String> {
    let sql = format!("SELECT toString(max({})) as max_time FROM {} FORMAT JSONEachRow", time_field, table);
    let rows = ch_query_rows(dsn, db, &sql).await?;
    Ok(rows.get(0).and_then(|r| r.get("max_time")).and_then(|v| v.as_str()).unwrap_or("").to_string())
}

// 获取时间范围（HTTP 方案）
async fn get_time_range_http(dsn: &str, db: &str, table: &str, time_field: &str, start: &str) -> anyhow::Result<(String, String)> {
    let sql = format!(
        "SELECT toString(min({})) as min_time, toString(max({})) as max_time FROM {} WHERE {} >= '{}' FORMAT JSONEachRow",
        time_field, time_field, table, time_field, start
    );
    let rows = ch_query_rows(dsn, db, &sql).await?;
    let min_time = rows.get(0).and_then(|r| r.get("min_time")).and_then(|v| v.as_str()).unwrap_or("").to_string();
    let max_time = rows.get(0).and_then(|r| r.get("max_time")).and_then(|v| v.as_str()).unwrap_or("").to_string();
    Ok((min_time, max_time))
}

// 获取行数据（HTTP 方案）
async fn get_rows_http(dsn: &str, db: &str, table: &str, time_field: &str, time_val: &str, col_names: &[String]) -> anyhow::Result<Vec<HashMap<String, Value>>> {
    let col_list = col_names.join(",");
    let sql = format!("SELECT {} FROM {} WHERE {} = '{}' FORMAT JSONEachRow", col_list, table, time_field, time_val);
    ch_query_rows(dsn, db, &sql).await
}

// 断点续传记录加载
fn load_done_segments(filename: &str) -> Result<HashSet<String>> {
    use std::io::{BufRead, BufReader};
    let mut done = HashSet::new();
    if let Ok(f) = File::open(filename) {
        let reader = BufReader::new(f);
        for line in reader.lines() {
            if let Ok(seg) = line {
                done.insert(seg);
            }
        }
    }
    Ok(done)
}

// 断点续传记录保存
fn save_done_segment(filename: &str, seg: &str) -> Result<()> {
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new().append(true).create(true).open(filename)?;
    writeln!(f, "{}", seg)?;
    Ok(())
}

// 分段生成（每小时一段，跳过已完成）
fn generate_hourly_segments_with_skip(min_time: &str, max_time: &str, done_segments: &HashSet<String>) -> Vec<String> {
    use chrono::NaiveDateTime;
    let mut segments = Vec::new();
    let min = NaiveDateTime::parse_from_str(min_time, "%Y-%m-%d %H:%M:%S").unwrap();
    let max = NaiveDateTime::parse_from_str(max_time, "%Y-%m-%d %H:%M:%S").unwrap();
    let mut t = min;
    while t < max {
        let seg = t.format("%Y-%m-%d %H:%M:%S").to_string();
        if !done_segments.contains(&seg) {
            segments.push(seg);
        }
        t += chrono::Duration::hours(1);
    }
    segments
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::from_args();
    // 先用 reqwest 直接测试 HTTP 认证
    if let Err(e) = test_reqwest_clickhouse_auth(&opt.src_dsn).await {
        eprintln!("[reqwest] ClickHouse HTTP 认证失败: {e}");
        return Err(e);
    }
    println!("datacp 启动，参数: {:?}", opt);
    let parallelism = opt.parallelism;
    let log_file_path = &opt.log_file;
    let ignore_fields = &opt.ignore_field;
    let done_segments_file = if !opt.done_segments.is_empty() {
        opt.done_segments.clone()
    } else {
        format!("done_segments_{}_to_{}.txt", opt.src_table, opt.dst_table)
    };
    let log_file = OpenOptions::new().create(true).append(true).open(log_file_path)?;
    let log_file = std::sync::Mutex::new(log_file);
    env_logger::Builder::from_default_env()
        .format(move |buf, record| {
            let mut log_file = log_file.lock().unwrap();
            let ts = chrono::Local::now().format("%Y-%m-%d %H:%M:%S");
            let log_line = format!(
                "{{\"time\":\"{}\",\"level\":\"{}\",\"msg\":\"{}\"}}\n",
                ts,
                record.level(),
                record.args()
            );
            let _ = log_file.write_all(log_line.as_bytes());
            let _ = log_file.flush(); // 强制落盘，防止日志丢失或混行
            writeln!(buf, "{}", log_line.trim_end())
        })
        .target(env_logger::Target::Stderr)
        .init();

    // 1. 表结构校验（传入 ignore_fields）
    compare_table_columns_http(
        &opt.src_dsn, &opt.src_db, &opt.dst_dsn, &opt.dst_db, &opt.src_table, &opt.dst_table, ignore_fields
    ).await?;
    // 2. 获取字段名，过滤 ignore_fields
    let all_col_names = get_column_names_http(&opt.src_dsn, &opt.src_db, &opt.src_table).await?;
    let col_names: Vec<String> = all_col_names.iter().filter(|c| !is_ignored_field(c, ignore_fields)).cloned().collect();
    let mut sorted_col_names = col_names.clone();
    sorted_col_names.sort();
    // 3. 校验时间字段
    if !col_names.contains(&opt.time_field) {
        error!("time_field {} 不存在于表结构", opt.time_field);
        return Err(anyhow::anyhow!("time_field 不存在"));
    }
    // 4. 获取时间范围
    info!("get_time_range SQL: SELECT min({}), max({}) FROM {} WHERE {} >= '{}'", opt.time_field, opt.time_field, opt.src_table, opt.time_field, opt.start_time);
    let (min_time, max_time) = get_time_range_http(&opt.src_dsn, &opt.src_db, &opt.src_table, &opt.time_field, &opt.start_time).await?;
    info!("get_time_range result: min_time='{}', max_time='{}'", min_time, max_time);
    if min_time.is_empty() || max_time.is_empty() {
        error!("数据源无数据，任务终止");
        return Ok(());
    }
    println!("min_time: {}, max_time: {}", min_time, max_time);
    // 5. 断点续传记录
    let done_segments = load_done_segments(&done_segments_file)?;
    // 6. 分段并发迁移主流程
    let segments = generate_hourly_segments_with_skip(&min_time, &max_time, &done_segments);
    let segment_chunks: Vec<Vec<String>> = segments.chunks((segments.len() + parallelism - 1) / parallelism).map(|c| c.to_vec()).collect();
    let mut handles = Vec::new();
    let client = Arc::new(reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(16)
        .build()?);
    for chunk in segment_chunks {
        let src_dsn = opt.src_dsn.clone();
        let dst_dsn = opt.dst_dsn.clone();
        let src_db = opt.src_db.clone();
        let dst_db = opt.dst_db.clone();
        let src_table = opt.src_table.clone();
        let dst_table = opt.dst_table.clone();
        let time_field = opt.time_field.clone();
        let col_names = col_names.clone();
        let sorted_col_names = sorted_col_names.clone();
        let ignore_fields = ignore_fields.clone();
        let done_segments_file = done_segments_file.clone();
        let log_file_path = log_file_path.clone();
        let client = client.clone();
        handles.push(tokio::spawn(migrate_segment_worker_http(
            chunk,
            src_dsn,
            dst_dsn,
            src_db,
            dst_db,
            src_table,
            dst_table,
            time_field,
            col_names,
            sorted_col_names,
            ignore_fields,
            done_segments_file,
            log_file_path,
            client.clone(),
        )));
    }
    join_all(handles).await;

    // 7. 增量迁移循环
    let mut cur_max_time = max_time.clone();
    loop {
        let (new_min, new_max) = get_time_range_http(&opt.src_dsn, &opt.src_db, &opt.src_table, &opt.time_field, &cur_max_time).await?;
        if new_min.is_empty() || new_max <= cur_max_time {
            info!("无新增数据，增量迁移完成");
            break;
        }
        info!("检测到新数据，增量迁移 {} ~ {}", new_min, new_max);
        let done_segments = load_done_segments(&done_segments_file)?;
        let segments = generate_hourly_segments_with_skip(&new_min, &new_max, &done_segments);
        let segment_chunks: Vec<Vec<String>> = segments.chunks((segments.len() + parallelism - 1) / parallelism).map(|c| c.to_vec()).collect();
        let mut handles = Vec::new();
        for chunk in segment_chunks {
            let src_dsn = opt.src_dsn.clone();
            let dst_dsn = opt.dst_dsn.clone();
            let src_db = opt.src_db.clone();
            let dst_db = opt.dst_db.clone();
            let src_table = opt.src_table.clone();
            let dst_table = opt.dst_table.clone();
            let time_field = opt.time_field.clone();
            let col_names = col_names.clone();
            let sorted_col_names = sorted_col_names.clone();
            let ignore_fields = ignore_fields.clone();
            let done_segments_file = done_segments_file.clone();
            let log_file_path = log_file_path.clone();
            let client = client.clone();
            handles.push(tokio::spawn(migrate_segment_worker_http(
                chunk, src_dsn, dst_dsn, src_db, dst_db, src_table, dst_table, time_field, col_names, sorted_col_names, ignore_fields, done_segments_file, log_file_path, client.clone(),
            )));
        }
        join_all(handles).await;
        cur_max_time = new_max;
    }
    // 8. _bak 补差与兜底增量、最终表切换
    // 8.1 rename 源表为 _bak
    let bak_table = format!("{}_bak", opt.src_table);
    let rename_sql = if opt.is_src_distributed && !opt.cluster_name.is_empty() {
        format!("RENAME TABLE {} TO {} ON CLUSTER {}", opt.src_table, bak_table, opt.cluster_name)
    } else {
        format!("RENAME TABLE {} TO {}", opt.src_table, bak_table)
    };
    if let Err(e) = ch_execute(&opt.src_dsn, &opt.src_db, &rename_sql).await {
        error!("重命名源表失败: {e}");
        return Err(anyhow::anyhow!(format!("重命名源表失败: {e}")));
    }
    // 8.2 获取 _bak 最大时间戳
    let bak_max_time = get_max_time_http(&opt.src_dsn, &opt.src_db, &bak_table, &opt.time_field).await?;
    // 8.3 _bak 补差写入
    let bak_rows = get_rows_http(&opt.src_dsn, &opt.src_db, &bak_table, &opt.time_field, &bak_max_time, &col_names).await?;
    let dst_rows = get_rows_http(&opt.dst_dsn, &opt.dst_db, &opt.dst_table, &opt.time_field, &bak_max_time, &col_names).await?;
    let dst_row_set: HashSet<String> = dst_rows.iter().map(|r| {
        let mut norm = serde_json::Map::new();
        for col in &sorted_col_names {
            let v = r.get(col).cloned().unwrap_or(Value::Null);
            norm.insert(col.clone(), v);
        }
        let b = serde_json::to_vec(&norm).unwrap();
        let mut hasher = Sha256::new();
        hasher.update(&b);
        format!("{:x}", hasher.finalize())
    }).collect();
    let mut need_insert = Vec::new();
    for row in bak_rows.iter() {
        let mut norm = serde_json::Map::new();
        for col in &sorted_col_names {
            let v = row.get(col).cloned().unwrap_or(Value::Null);
            norm.insert(col.clone(), v);
        }
        let b = serde_json::to_vec(&norm).unwrap();
        let mut hasher = Sha256::new();
        hasher.update(&b);
        let key = format!("{:x}", hasher.finalize());
        if !dst_row_set.contains(&key) {
            need_insert.push(row.clone());
        }
    }
    if !need_insert.is_empty() {
        for batch in need_insert.chunks(1000) {
            let json_rows: Vec<String> = batch.iter().map(|row| serde_json::to_string(row).unwrap()).collect();
            let data = json_rows.join("\n");
            insert_rows_http(&opt.dst_dsn, &opt.dst_db, &opt.dst_table, data).await?;
        }
    }
    // 8.4 _bak 兜底增量迁移
    let bak_min_time = chrono::NaiveDateTime::parse_from_str(&bak_max_time, "%Y-%m-%d %H:%M:%S").unwrap() + chrono::Duration::nanoseconds(1);
    let bak_min_time_str = bak_min_time.format("%Y-%m-%d %H:%M:%S").to_string();
    let (bak_new_min, bak_new_max) = get_time_range_http(&opt.src_dsn, &opt.src_db, &bak_table, &opt.time_field, &bak_min_time_str).await?;
    if !bak_new_min.is_empty() && bak_new_max > bak_max_time {
        let segments = generate_hourly_segments_with_skip(&bak_new_min, &bak_new_max, &HashSet::new());
        let segment_chunks: Vec<Vec<String>> = segments.chunks((segments.len() + parallelism - 1) / parallelism).map(|c| c.to_vec()).collect();
        let mut handles = Vec::new();
        for chunk in segment_chunks {
            handles.push(tokio::spawn(migrate_segment_worker_http(
                chunk,
                opt.src_dsn.clone(),
                opt.dst_dsn.clone(),
                opt.src_db.clone(),
                opt.dst_db.clone(),
                bak_table.clone(),
                opt.dst_table.clone(),
                opt.time_field.clone(),
                col_names.clone(),
                sorted_col_names.clone(),
                ignore_fields.clone(),
                done_segments_file.clone(),
                log_file_path.clone(),
                client.clone(),
            )));
        }
        join_all(handles).await;
    }
    // 8.5 rename 目标表为 src_table
    let rename_dst_sql = if opt.is_dst_distributed && !opt.cluster_name.is_empty() {
        format!("RENAME TABLE {} TO {} ON CLUSTER {}", opt.dst_table, opt.src_table, opt.cluster_name)
    } else {
        format!("RENAME TABLE {} TO {}", opt.dst_table, opt.src_table)
    };
    if let Err(e) = ch_execute(&opt.dst_dsn, &opt.dst_db, &rename_dst_sql).await {
        error!("重命名目标表失败: {e}");
        return Err(anyhow::anyhow!(format!("重命名目标表失败: {e}")));
    }
    // 8.6 done_segments 文件重命名
    if std::path::Path::new(&done_segments_file).exists() {
        let ts = chrono::Local::now().format("%Y%m%d_%H%M%S");
        let new_name = format!("{}_{}.txt", done_segments_file.trim_end_matches(".txt"), ts);
        std::fs::rename(&done_segments_file, &new_name)?;
    }
    info!("最终切换完成，迁移流程结束");
    Ok(())
}