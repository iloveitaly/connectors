use anyhow::Context;

use crate::convert::Convert;

pub fn load_spec(
    source: &str,
) -> anyhow::Result<(
    Vec<connector_protocol::CollectionSpec>,
    proto_flow::flow::MaterializationSpec,
)> {
    let dir = tempdir::TempDir::new("flowsim-build")
        .expect("could not create temp dir")
        .path()
        .canonicalize()
        .expect("could not canonicalize temp dir path")
        .into_os_string()
        .into_string()
        .expect("temp dir path must be valid unicode");

    let config = proto_flow::flow::build_api::Config {
        build_id: "flowsim-build".to_string(),
        directory: dir,
        source: source.to_string(),
        ..Default::default()
    };

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("could not build runtime");

    let mut tables = runtime
        .block_on(build::configured_build(
            config,
            Fetcher {},
            validation::NoOpDrivers {}, // TODO: Could actually use a driver for this maybe.
        ))
        .context("extracting tables from catalog specification")?;

    let collections = tables
        .built_collections
        .drain(..)
        .filter(|built| {
            // Remove ops collections that are injected into the build.
            built.collection.as_str() != "ops.us-central1.v1/logs"
                && built.collection.as_str() != "ops.us-central1.v1/stats"
        })
        .map(|built| built.spec.convert())
        .collect();

    if tables.built_materializations.len() != 1 {
        anyhow::bail!(
            "catalog spec must contain a single materialization, found {} materialization specs",
            tables.built_materializations.len()
        )
    }

    let mat = tables.built_materializations.pop().unwrap().spec.clone();

    Ok((collections, mat))
}

struct Fetcher;

impl sources::Fetcher for Fetcher {
    fn fetch<'a>(
        &'a self,
        // Resource to fetch.
        resource: &'a url::Url,
        // Expected content type of the resource.
        _content_type: proto_flow::flow::ContentType,
    ) -> sources::FetchFuture<'a> {
        let url = resource.clone();
        Box::pin(fetch_async(url))
    }
}

async fn fetch_async(resource: url::Url) -> Result<bytes::Bytes, anyhow::Error> {
    match resource.scheme() {
        // "http" | "https" => {
        //     let resp = reqwest::get(resource.as_str()).await?;
        //     let status = resp.status();

        //     if status.is_success() {
        //         Ok(resp.bytes().await?)
        //     } else {
        //         let body = resp.text().await?;
        //         anyhow::bail!("{status}: {body}");
        //     }
        // }
        "file" => {
            let path = resource
                .to_file_path()
                .map_err(|err| anyhow::anyhow!("failed to convert file uri to path: {:?}", err))?;

            let bytes =
                std::fs::read(path).with_context(|| format!("failed to read {resource}"))?;
            Ok(bytes.into())
        }
        _ => Err(anyhow::anyhow!(
            "cannot fetch unsupported URI scheme: '{resource}'"
        )),
    }
}
