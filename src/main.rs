use anyhow::Result;
use log::{info, LevelFilter};
use log4rs::append::console::ConsoleAppender;

use log4rs::config::{Appender, Config, Root};
use log4rs::encode::pattern::PatternEncoder;

use submiter::submitter::monitor_and_submit;
mod submiter;

fn init_logging() {
    // 从环境变量获取日志级别，默认为 Info
    let log_level = std::env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string());
    // 从环境变量获取日志输出类型，默认为 console
    let log_output = std::env::var("LOG_CONFIG").unwrap_or_else(|_| "console".to_string());

    if log_output.as_str() == "file" {
        log4rs::init_file("config/log4rs.yaml", Default::default()).unwrap();
    } else {
        let level_filter = match log_level.as_str() {
            "error" => LevelFilter::Error,
            "warn" => LevelFilter::Warn,
            "info" => LevelFilter::Info,
            "debug" => LevelFilter::Debug,
            "trace" => LevelFilter::Trace,
            _ => LevelFilter::Info, // 默认级别
        };

        let console_appender = ConsoleAppender::builder()
            .encoder(Box::new(PatternEncoder::new(
                "{d} {h({l})} {M}:{L} - {m}{n}",
            )))
            .build();

        let config_builder = Config::builder();

        let config = config_builder
            .appender(Appender::builder().build("console", Box::new(console_appender)))
            .build(Root::builder().appender("console").build(level_filter))
            .unwrap();
        log4rs::init_config(config).unwrap();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // 加载 .env 文件中的环境变量
    dotenv::dotenv().ok();
    init_logging();
    info!("Starting the submiter application...");
    monitor_and_submit().await
}
