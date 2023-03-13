use std::{
    collections::HashMap,
    io::{LineWriter, Write},
    sync::{Arc, Mutex},
};

use anyhow::Context;
use rand::Rng;
use tuple::TuplePack;

use {
    connector_protocol::materialize as protocol,
    std::io::{BufRead, BufReader},
    std::process::{Command, Stdio},
    std::thread,
    std::{
        process::{ChildStdin, ChildStdout},
        sync::mpsc::{self},
    },
};

fn main() -> anyhow::Result<()> {
    // let mut connector = Command::new("docker")
    //     .args(["run", "-i", "ghcr.io/estuary/materialize-json:local"])
    //     .stdin(Stdio::piped())
    //     .stdout(Stdio::piped())
    //     // TODO: Route logs
    //     // .stderr(Stdio::piped())
    //     .spawn()
    //     .expect("failed to start container");

    let mut connector = Command::new("go")
        .args([
            "run",
            "/Users/wbaker/estuary/connectors/materialize-json",
            "--log.level",
            "debug",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        // TODO: Route logs
        // .stderr(Stdio::piped())
        .spawn()
        .expect("failed to start container");

    let input = connector.stdin.take().unwrap();
    let output = connector.stdout.take().unwrap();

    let (tx_commands, rx_commands) = mpsc::sync_channel(0);
    let (tx_responses, rx_responses) = mpsc::sync_channel(0);

    let threads = vec![
        thread::spawn(move || handle_commands(input, rx_commands)),
        thread::spawn(move || handle_responses(output, tx_responses)),
    ];

    let (_collection_spec, materialization_spec) =
        flowsim::spec::load_spec("file:///Users/wbaker/estuary/connectors/flowsim/flow.yaml")?;

    let apply_bindings = open_transactions(&tx_commands, &rx_responses, &materialization_spec)?;
    run_transactions(tx_commands, rx_responses, &apply_bindings)?;

    connector.wait()?;

    for join in threads {
        join.join()
            .expect("thread panic")
            .context("joining threads")?;
    }

    Ok(())
}

fn handle_commands(
    connector_input: ChildStdin,
    commands: mpsc::Receiver<protocol::Request>,
) -> anyhow::Result<()> {
    let mut writer = LineWriter::new(connector_input);

    while let Some(command) = commands.iter().next() {
        serde_json::to_writer(&mut writer, &command)?;
        writeln!(&mut writer)?;
    }

    Ok(())
}

fn handle_responses(
    connector_output: ChildStdout,
    responses: mpsc::SyncSender<protocol::Response>,
) -> anyhow::Result<()> {
    let mut reader = BufReader::new(connector_output);
    let mut line = String::new();

    loop {
        line.clear();

        match reader.read_line(&mut line) {
            Ok(0) => break, // EOF
            Ok(_) => responses.send(serde_json::from_str(&line)?)?,
            Err(e) => anyhow::bail!(e),
        }
    }

    Ok(())
}

fn open_transactions(
    tx_commands: &mpsc::SyncSender<protocol::Request>,
    rx_responses: &mpsc::Receiver<protocol::Response>,
    materialization_spec: &proto_flow::flow::MaterializationSpec,
) -> anyhow::Result<Vec<protocol::ApplyBinding>> {
    // Make sure the spec command works...?
    // Maybe try to match it up with the provided spec.
    tx_commands.send(protocol::Request::Spec {})?;
    match rx_responses.recv()? {
        protocol::Response::Spec {
            documentation_url: _,
            config_schema: _,
            resource_config_schema: _,
            oauth2: _,
        } => (),
        _ => panic!("protocol error, expected spec"),
    };

    // Validate the provided spec so we can get some bindings to use.
    tx_commands.send(flowsim::validate_request(&materialization_spec))?;
    let validated_bindings = match rx_responses.recv().context("getting validated")? {
        protocol::Response::Validated { bindings } => bindings,
        _ => panic!("protocol error, expected validated"),
    };

    // Apply the spec
    tx_commands.send(flowsim::apply_request(
        &materialization_spec,
        &validated_bindings,
    ))?;
    let action_description = match rx_responses.recv().context("getting applied")? {
        protocol::Response::Applied { action_description } => action_description,
        _ => panic!("protocol error, expected applied"),
    };

    println!(
        "Connector applied action description: {}",
        action_description
    );

    // Send open
    tx_commands.send(flowsim::open_request(
        &materialization_spec,
        &validated_bindings,
    ))?;
    let runtime_checkpoint = match rx_responses.recv().context("getting opened")? {
        protocol::Response::Opened { runtime_checkpoint } => runtime_checkpoint,
        _ => panic!("protocol error, expected opened"),
    };

    println!(
        "Connector reported runtime checkpoint: {}",
        runtime_checkpoint
    );

    Ok(flowsim::apply_bindings(
        &materialization_spec,
        &validated_bindings,
    ))
}

struct StoredValue {
    binding: u32,
    key_packed: String,
    key: Vec<serde_json::Value>,
    doc: connector_protocol::RawValue,
}

impl PartialEq for StoredValue {
    fn eq(&self, other: &Self) -> bool {
        let my_doc: serde_json::Value = serde_json::from_str(self.doc.get()).unwrap();
        let other_doc: serde_json::Value = serde_json::from_str(other.doc.get()).unwrap();

        self.binding == other.binding
            && self.key_packed == other.key_packed
            && self.key == other.key
            && my_doc == other_doc
    }
}

impl Eq for StoredValue {}

struct NewValue {
    stored: StoredValue,
    values: Vec<serde_json::Value>,
}

// each binding has a KV
struct RuntimeStore {
    // stored represents what the runtime has committed
    stored: HashMap<u32, BindingStore>,
    // loaded will be reset every cycle
    loaded: HashMap<u32, BindingStore>,
}

impl RuntimeStore {
    fn new() -> Self {
        RuntimeStore {
            stored: HashMap::new(),
            loaded: HashMap::new(),
        }
    }
}

struct BindingStore {
    // Packed key: Stored Value
    docs: HashMap<String, StoredValue>,
}

impl BindingStore {
    fn new() -> Self {
        BindingStore {
            docs: HashMap::new(),
        }
    }
}

fn run_transactions(
    tx_commands: mpsc::SyncSender<protocol::Request>,
    rx_responses: mpsc::Receiver<protocol::Response>,
    bindings: &[protocol::ApplyBinding],
) -> anyhow::Result<()> {
    let runtime_store = Arc::new(Mutex::new(RuntimeStore::new()));
    let thread_runtime_store = runtime_store.clone();

    let ack_parker = crossbeam::sync::Parker::new();
    let ack_unparker = ack_parker.unparker().clone();
    let flush_parker = crossbeam::sync::Parker::new();
    let flush_unparker = flush_parker.unparker().clone();
    let started_commit_parker = crossbeam::sync::Parker::new();
    let started_commit_unparker = started_commit_parker.unparker().clone();

    let binding_keys = bindings
        .iter()
        .enumerate()
        .map(|(idx, binding)| {
            let key_ref = &binding.collection.key;
            (idx as u32, key_ref.to_owned())
        })
        .collect::<HashMap<u32, Vec<String>>>();

    // need to be able to received acknowledged, loaded, flushed, startedCommit basically at any time
    let responses = thread::spawn(move || {
        while let Some(response) = rx_responses.iter().next() {
            match response {
                protocol::Response::Acknowledged {} => {
                    ack_unparker.unpark();
                }
                protocol::Response::Loaded { binding, doc } => {
                    // Decode the doc, get its keys, and work them into our store
                    let val_doc: serde_json::Value = serde_json::from_str(doc.get()).unwrap();

                    let key_ptrs = &binding_keys[&binding];

                    let mut arena: Vec<u8> = Vec::new();
                    let mut key: Vec<serde_json::Value> = Vec::new();
                    for k in key_ptrs {
                        let ptr = doc::Pointer::from_str(k);
                        let val = ptr.query(&val_doc).unwrap();
                        val.pack(&mut arena, tuple::TupleDepth::new().increment())
                            .expect("vec<u8> never fails to write");
                        key.push(val.to_owned());
                    }
                    let key_packed = hex::encode(arena);

                    let mut store = thread_runtime_store.lock().unwrap();

                    if store.loaded.get(&binding).is_none() {
                        store.loaded.insert(binding, BindingStore::new());
                    }

                    if !store
                        .loaded
                        .get_mut(&binding)
                        .unwrap()
                        .docs
                        .insert(
                            key_packed.clone(),
                            StoredValue {
                                binding,
                                key_packed,
                                key,
                                doc: *doc,
                            },
                        )
                        .is_none()
                    {
                        panic!("received duplicate loaded key in load phase");
                    }
                }
                protocol::Response::Flushed {} => {
                    flush_unparker.unpark();
                }
                protocol::Response::StartedCommit {
                    driver_checkpoint: _,
                    merge_patch: _,
                } => {
                    // This means we can start pipling the next transaction while the current one is
                    // commiting. We should commit to our recovery log and send Acknowledge, and
                    // then immediately start sending load requests.
                    started_commit_unparker.unpark();
                }
                _ => panic!("protocol error"),
            }
        }
    });

    let mut count = 0;
    loop {
        count = count + 1;
        if count > 10 {
            break;
        }
        println!("loop {}", count);
        // Clear the loaded store for this round. The lock is immediately dropped.
        runtime_store.lock().unwrap().loaded.clear();

        // Send acknowledge. We don't expect to get anything back from this directly. This means we have started our commit.
        tx_commands.send(protocol::Request::Acknowledge {})?;

        let updates = pick_docs_to_update(runtime_store.clone());
        let new = make_new_docs(bindings.len() as u32, count);

        // Immediately send load. If its delta we don't send any load but will need to send Flush and get Flushed back.
        for load in new.iter().map(|n| &n.stored).chain(updates.iter()) {
            tx_commands.send(protocol::Request::Load {
                binding: load.binding,
                key_packed: load.key_packed.clone(),
                key: load.key.clone(),
            })?;
        }

        // Send flush only if we have received acknowledged
        ack_parker.park();
        tx_commands.send(protocol::Request::Flush {})?;

        // Send store only if we have received flushed
        flush_parker.park();

        let updated_docs = make_updated_docs(&updates, count);

        for store in updated_docs.iter() {
            tx_commands.send(protocol::Request::Store {
                binding: store.stored.binding,
                key_packed: store.stored.key_packed.clone(),
                key: store.stored.key.clone(),
                values: store.values.clone(),
                doc: Box::new(flowsim::raw_value(store.stored.doc.get())),
                exists: true,
            })?;
        }

        for store in new.iter() {
            tx_commands.send(protocol::Request::Store {
                binding: store.stored.binding,
                key_packed: store.stored.key_packed.clone(),
                key: store.stored.key.clone(),
                values: store.values.clone(),
                doc: Box::new(flowsim::raw_value(store.stored.doc.get())),
                exists: false,
            })?;
        }

        // Send start commit, then wait for started commit before looping back around.
        tx_commands.send(protocol::Request::StartCommit {
            runtime_checkpoint: "".to_string(),
        })?;

        started_commit_parker.park();
        for new in new.into_iter() {
            let mut store = runtime_store.lock().unwrap();
            if store.stored.get(&new.stored.binding).is_none() {
                store.stored.insert(new.stored.binding, BindingStore::new());
            }

            if store
                .stored
                .get_mut(&new.stored.binding)
                .unwrap()
                .docs
                .insert(new.stored.key_packed.clone(), new.stored)
                .is_some()
            {
                panic!("storing a new document that already exists");
            }
        }

        for updated in updated_docs.into_iter() {
            let mut store = runtime_store.lock().unwrap();
            if store.stored.get(&updated.stored.binding).is_none() {
                store
                    .stored
                    .insert(updated.stored.binding, BindingStore::new());
            }

            if store
                .stored
                .get_mut(&updated.stored.binding)
                .unwrap()
                .docs
                .insert(updated.stored.key_packed.clone(), updated.stored)
                .is_none()
            {
                panic!("updating a document that doesn't already exist");
            }
        }
    }

    // At some point here we should validate that the endpoint contains everything it should.
    println!("all done, validating data");
    tx_commands.send(protocol::Request::Acknowledge {})?;

    // Send a load with all of our keys, then a flush
    let mut all_keys: Vec<StoredValue> = Vec::new();
    let mut store = runtime_store.lock().unwrap();
    for (_, binding_store) in store.stored.iter() {
        for (_, stored) in binding_store.docs.iter() {
            all_keys.push(StoredValue {
                binding: stored.binding,
                key_packed: stored.key_packed.clone(),
                key: stored.key.clone(),
                doc: flowsim::raw_value(stored.doc.get()),
            });
        }
    }
    store.loaded.clear();
    drop(store);

    for stored in all_keys.iter() {
        tx_commands.send(protocol::Request::Load {
            binding: stored.binding,
            key_packed: stored.key_packed.clone(),
            key: stored.key.clone(),
        })?;
    }

    // Make sure the connecotor has finished committing, per the protocol.
    ack_parker.park();
    tx_commands.send(protocol::Request::Flush {})?;
    flush_parker.park();

    // Now compare what we loaded vs. what we have stored.
    let store = runtime_store.lock().unwrap();
    let stored = &store.stored;
    let loaded = &store.loaded;

    for (binding, binding_store) in stored.iter() {
        for (packed_key, stored_doc) in binding_store.docs.iter() {
            let loaded_doc = loaded.get(&binding).unwrap().docs.get(packed_key).unwrap();
            if stored_doc != loaded_doc {
                panic!("loaded from connector differs from stored in runtime");
            }
        }
    }
    println!("stored docs all match loaded from connector");

    for (binding, binding_store) in loaded.iter() {
        for (packed_key, loaded_doc) in binding_store.docs.iter() {
            let stored_doc = stored.get(&binding).unwrap().docs.get(packed_key).unwrap();
            if stored_doc != loaded_doc {
                panic!("loaded from connector differs from stored in runtime");
            }
        }
    }
    println!("loaded docs all match stored in runtime");

    drop(tx_commands);
    responses.join().expect("thread panic");
    Ok(())
}

// Update this many existing docs (if there are that many), and add this many new docs each round.
const DOC_CONST: usize = 5;

fn pick_docs_to_update(runtime_store: Arc<Mutex<RuntimeStore>>) -> Vec<StoredValue> {
    // Return the keys to load
    // Some other thing will pick the new values
    let store = runtime_store.lock().unwrap();

    // Just list out all of the values and pick randomly. This is really inefficient.
    let mut all_vals: Vec<StoredValue> = store
        .stored
        .iter()
        .flat_map(|(_, binding_store)| {
            binding_store.docs.iter().map(|(_, stored)| StoredValue {
                binding: stored.binding.clone(),
                key_packed: stored.key_packed.clone(),
                key: stored.key.clone(),
                doc: flowsim::raw_value(stored.doc.get()),
            })
        })
        .collect();

    let mut picked: Vec<StoredValue> = Vec::new();
    let mut rng = rand::thread_rng();
    for _ in 0..DOC_CONST {
        if all_vals.len() == 0 {
            break;
        }

        let idx: usize = rng.gen_range(0..all_vals.len());
        picked.push(all_vals.swap_remove(idx));
    }

    picked
}

// num_bindings should be length of the bindings. its 0 indexed and non-inclusive on the upper end
fn make_new_docs(num_bindings: u32, round: usize) -> Vec<NewValue> {
    let mut out = Vec::new();

    let mut rng = rand::thread_rng();
    for idx in 0..DOC_CONST {
        let key = vec![
            serde_json::to_value(round).unwrap(),
            serde_json::to_value(format!("bob_round_{}_idx_{}", round, idx)).unwrap(),
        ];

        let values =
            vec![serde_json::to_value(format!("hello round {} idx {}", round, idx)).unwrap()];

        let mut arena: Vec<u8> = Vec::new();
        for k in &key {
            k.pack(&mut arena, tuple::TupleDepth::new().increment())
                .expect("vec<u8> never fails to write");
        }

        let doc = serde_json::json!({
                "count": round,
                "counter": format!("bob_round_{}_idx_{}", round, idx),
                "message": format!("hello round {} idx {}", round, idx),
        });

        out.push(NewValue {
            stored: StoredValue {
                binding: rng.gen_range(0..num_bindings),
                key_packed: hex::encode(arena),
                key,
                doc: flowsim::raw_value(&doc.to_string()),
            },
            values,
        });
    }

    out
}

fn make_updated_docs(stored: &[StoredValue], round: usize) -> Vec<NewValue> {
    // All we can do is update the message

    stored
        .iter()
        .map(|s| {
            let keys = s.key.clone();

            let values =
                vec![serde_json::to_value(format!("last updated in round {}", round)).unwrap()];

            let doc = serde_json::json!({
                    "count": keys[0],
                    "counter": keys[1],
                    "message": values[0],
            });

            NewValue {
                stored: StoredValue {
                    binding: s.binding,
                    key_packed: s.key_packed.clone(),
                    key: s.key.clone(),
                    doc: flowsim::raw_value(&doc.to_string()),
                },
                values,
            }
            //
        })
        .collect()
}
