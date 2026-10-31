//! Offline delivery contracts: no relay, model, runtime or credential side effects.
use std::collections::VecDeque;
use std::sync::Mutex;

use buzz_acp::reply_delivery::{
    is_silence, Delivery, Destination, Operation, Outcome, Reservation, Transport,
};
use buzz_acp::FinalReplyCapture;
use nostr::{Event, EventBuilder, Keys, Kind};
use serde_json::{json, Value};
use uuid::Uuid;

#[derive(Default)]
struct Relay {
    reads: Mutex<VecDeque<Result<Value, &'static str>>>,
    sent: Mutex<Vec<Event>>,
    filters: Mutex<Vec<Value>>,
    ack: &'static str,
}

impl Relay {
    fn new(reads: Vec<Result<Value, &'static str>>, ack: &'static str) -> Self {
        Self {
            reads: Mutex::new(reads.into()),
            ack,
            ..Self::default()
        }
    }
}

impl Transport for Relay {
    fn query(&self, filter: Value) -> Operation<'_> {
        self.filters.lock().unwrap().push(filter);
        let result = self
            .reads
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Err("unexpected_query"));
        Box::pin(async move { result })
    }
    fn submit(&self, event: &Event) -> Operation<'_> {
        self.sent.lock().unwrap().push(event.clone());
        let result = match self.ack {
            "accepted" => Ok(json!({"event_id":event.id.to_hex(),"accepted":true})),
            "rejected" => Ok(json!({"event_id":event.id.to_hex(),"accepted":false})),
            "wrong_id" => Ok(json!({"event_id":"wrong","accepted":true})),
            _ => Err("ack_lost"),
        };
        Box::pin(async move { result })
    }
}

struct Fixture {
    dir: std::path::PathBuf,
    keys: Keys,
    dest: Destination,
}
impl Fixture {
    fn new() -> Self {
        Self {
            dir: std::env::temp_dir().join(format!("buzz-reply-test-{}", Uuid::new_v4())),
            keys: Keys::generate(),
            dest: Destination {
                channel: Uuid::new_v4(),
                root: None,
                parent: None,
            },
        }
    }
    fn reserve(&self) -> Reservation {
        Reservation::acquire(&self.dir, "stable-input-batch").unwrap()
    }
    fn reply(&self, text: &str) -> Event {
        buzz_sdk::builders::build_message(self.dest.channel, text, None, &[], false, &[])
            .unwrap()
            .sign_with_keys(&self.keys)
            .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.dir).ok();
    }
}

fn update(kind: &str, text: &str) -> Value {
    json!({"params":{"sessionId":"s","update":{"sessionUpdate":kind,"content":{"type":"text","text":text}}}})
}

#[test]
fn capture_final_only_and_consume() {
    let mut c = FinalReplyCapture::default();
    c.begin("s");
    c.update(&update("agent_thought_chunk", "private thought"));
    c.update(&update("agent_message_chunk", "pre-tool narration"));
    c.update(&json!({"params":{"sessionId":"s","update":{"sessionUpdate":"tool_call","toolCallId":"t","status":"in_progress"}}}));
    c.update(&update("agent_message_chunk", "during tool"));
    c.update(&json!({"params":{"sessionId":"s","update":{"sessionUpdate":"tool_call_update","toolCallId":"t","status":"completed","content":[{"text":"secret output"}]}}}));
    c.update(&update("agent_message_chunk", "final "));
    c.update(&update("agent_message_chunk", "answer"));
    assert_eq!(c.finish(true).unwrap(), "final answer");
    assert_eq!(c.finish(true).unwrap(), "");
}

#[test]
fn capture_refuses_overflow_unfinished_tool_and_non_end_turn() {
    let mut c = FinalReplyCapture::default();
    c.begin("s");
    c.update(&update("agent_message_chunk", &"é".repeat(32769)));
    assert!(c.finish(true).is_err());
    c.begin("s");
    c.update(&update("agent_message_chunk", "answer"));
    assert!(c.finish(false).is_err());
    c.begin("s");
    c.update(&json!({"params":{"sessionId":"s","update":{"sessionUpdate":"tool_call","toolCallId":"t"}}}));
    assert!(c.finish(true).is_err());
    c.begin("s");
    c.update(&update("agent_message_chunk", "answer"));
    c.invalidate();
    assert!(c.finish(true).is_err());
}

#[test]
fn capture_session_and_typed_content_isolation() {
    let mut c = FinalReplyCapture::default();
    c.begin("other");
    c.update(&update("agent_message_chunk", "wrong session"));
    assert_eq!(c.finish(true).unwrap(), "");
    c.begin("s");
    let mut u = update("agent_message_chunk", "resource text");
    u["params"]["update"]["content"]["type"] = json!("resource");
    c.update(&u);
    assert_eq!(c.finish(true).unwrap(), "");
}

#[tokio::test]
async fn no_tool_final_one_reply() {
    let f = Fixture::new();
    let r = Relay::new(vec![Ok(json!([])), Ok(json!([]))], "accepted");
    let d = Delivery::begin(&r, f.dest.clone(), &f.keys, f.reserve())
        .await
        .unwrap();
    let outcome = d.finish(&r, &f.keys, Ok("exact final".into())).await;
    let sent = r.sent.lock().unwrap();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].content, "exact final");
    assert_eq!(outcome, Outcome::Accepted(sent[0].id));
    assert!(sent[0].verify().is_ok());
    assert!(Reservation::acquire(&f.dir, "stable-input-batch").is_err());
}

#[tokio::test]
async fn cli_reply_missing_or_delayed_self_echo_no_extra() {
    // No self-echo is supplied to this state machine: authenticated readback suffices.
    let f = Fixture::new();
    let reply = f.reply("CLI sent");
    let r = Relay::new(vec![Ok(json!([])), Ok(json!([reply]))], "accepted");
    let d = Delivery::begin(&r, f.dest.clone(), &f.keys, f.reserve())
        .await
        .unwrap();
    assert_eq!(
        d.finish(&r, &f.keys, Ok("final".into())).await,
        Outcome::Existing(reply.id)
    );
    assert!(r.sent.lock().unwrap().is_empty());
}

#[tokio::test]
async fn old_reply_is_not_this_turn() {
    let f = Fixture::new();
    let old = f.reply("previous turn");
    let r = Relay::new(vec![Ok(json!([old])), Ok(json!([old]))], "accepted");
    let d = Delivery::begin(&r, f.dest.clone(), &f.keys, f.reserve())
        .await
        .unwrap();
    assert!(matches!(
        d.finish(&r, &f.keys, Ok("new turn".into())).await,
        Outcome::Accepted(_)
    ));
    assert_eq!(r.sent.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn rejection_no_retry() {
    let f = Fixture::new();
    let r = Relay::new(vec![Ok(json!([])), Ok(json!([]))], "rejected");
    let d = Delivery::begin(&r, f.dest.clone(), &f.keys, f.reserve())
        .await
        .unwrap();
    assert_eq!(
        d.finish(&r, &f.keys, Ok("answer".into())).await,
        Outcome::Failed("relay_rejected")
    );
    assert_eq!(r.sent.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn duplicate_query_uncertainty_fails_closed() {
    for failure in [
        Err("query_timeout"),
        Ok(json!({"not":"events"})),
        Ok(json!(vec![Value::Null; 32])),
    ] {
        let f = Fixture::new();
        let r = Relay::new(vec![Ok(json!([])), failure], "accepted");
        let d = Delivery::begin(&r, f.dest.clone(), &f.keys, f.reserve())
            .await
            .unwrap();
        assert!(matches!(
            d.finish(&r, &f.keys, Ok("answer".into())).await,
            Outcome::Uncertain(_)
        ));
        assert!(r.sent.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn baseline_failure_never_authorizes_delivery() {
    let f = Fixture::new();
    let r = Relay::new(vec![Err("offline")], "accepted");
    assert!(Delivery::begin(&r, f.dest.clone(), &f.keys, f.reserve())
        .await
        .is_err());
    assert!(r.sent.lock().unwrap().is_empty());
}

#[tokio::test]
async fn empty_silence_and_cancel_never_publish() {
    for text in [
        "",
        "NO_REPLY",
        "NO_RESPONSE",
        "[silent]",
        "HEARTBEAT_OK",
        " (silence) ",
    ] {
        assert!(is_silence(text));
        let f = Fixture::new();
        let r = Relay::new(vec![Ok(json!([])), Ok(json!([]))], "accepted");
        let d = Delivery::begin(&r, f.dest.clone(), &f.keys, f.reserve())
            .await
            .unwrap();
        assert_eq!(
            d.finish(&r, &f.keys, Ok(text.into())).await,
            Outcome::Silent
        );
        assert!(r.sent.lock().unwrap().is_empty());
    }
    let f = Fixture::new();
    let r = Relay::new(vec![Ok(json!([]))], "accepted");
    let d = Delivery::begin(&r, f.dest.clone(), &f.keys, f.reserve())
        .await
        .unwrap();
    assert_eq!(
        d.finish(&r, &f.keys, Err("non_end_turn")).await,
        Outcome::Failed("non_end_turn")
    );
    assert!(r.sent.lock().unwrap().is_empty());
}

struct LostAckRelay {
    inner: Relay,
}
impl Transport for LostAckRelay {
    fn query(&self, filter: Value) -> Operation<'_> {
        if filter.get("ids").is_some() {
            self.inner.filters.lock().unwrap().push(filter.clone());
            let sent = self.inner.sent.lock().unwrap()[0].clone();
            assert_eq!(filter["ids"], json!([sent.id.to_hex()]));
            return Box::pin(async move { Ok(json!([sent])) });
        }
        self.inner.query(filter)
    }
    fn submit(&self, event: &Event) -> Operation<'_> {
        self.inner.submit(event)
    }
}

#[tokio::test]
async fn ack_loss_reconciles_identical_signed_id_once() {
    let f = Fixture::new();
    let r = LostAckRelay {
        inner: Relay::new(vec![Ok(json!([])), Ok(json!([]))], "lost"),
    };
    let d = Delivery::begin(&r, f.dest.clone(), &f.keys, f.reserve())
        .await
        .unwrap();
    let o = d.finish(&r, &f.keys, Ok("answer".into())).await;
    let sent = r.inner.sent.lock().unwrap();
    assert_eq!(sent.len(), 1);
    assert_eq!(o, Outcome::Accepted(sent[0].id));
    let record = std::fs::read_dir(&f.dir)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let persisted: Event = serde_json::from_slice(&std::fs::read(record).unwrap()).unwrap();
    assert_eq!(persisted, sent[0]);
}

#[tokio::test]
async fn ack_loss_absence_or_wrong_ack_is_uncertain_no_resign() {
    for ack in ["lost", "wrong_id"] {
        let f = Fixture::new();
        let r = Relay::new(vec![Ok(json!([])), Ok(json!([])), Ok(json!([]))], ack);
        let d = Delivery::begin(&r, f.dest.clone(), &f.keys, f.reserve())
            .await
            .unwrap();
        assert!(matches!(
            d.finish(&r, &f.keys, Ok("answer".into())).await,
            Outcome::Uncertain(_)
        ));
        assert_eq!(r.sent.lock().unwrap().len(), 1);
        assert!(Reservation::acquire(&f.dir, "stable-input-batch").is_err());
    }
}

#[test]
fn crash_before_or_after_publication_reservation_survives() {
    let f = Fixture::new();
    drop(f.reserve());
    assert!(Reservation::acquire(&f.dir, "stable-input-batch").is_err());
    // A second process/racing retry cannot create a reservation for the same input.
    let dir = f.dir.clone();
    assert!(
        std::thread::spawn(move || Reservation::acquire(&dir, "stable-input-batch").is_err())
            .join()
            .unwrap()
    );
}

#[tokio::test]
async fn foreign_signature_or_kind_does_not_prove_accepted_reply() {
    let f = Fixture::new();
    let foreign = EventBuilder::new(Kind::Custom(9), "foreign")
        .sign_with_keys(&Keys::generate())
        .unwrap();
    let r = Relay::new(vec![Ok(json!([])), Ok(json!([foreign]))], "accepted");
    let d = Delivery::begin(&r, f.dest.clone(), &f.keys, f.reserve())
        .await
        .unwrap();
    assert!(matches!(
        d.finish(&r, &f.keys, Ok("answer".into())).await,
        Outcome::Uncertain(_)
    ));
    assert!(r.sent.lock().unwrap().is_empty());
}

#[tokio::test]
async fn incomplete_destination_and_signer_drift_fail_closed() {
    let f = Fixture::new();
    let r = Relay::new(vec![Ok(json!([]))], "accepted");
    let mut invalid = f.dest.clone();
    invalid.root = Some(nostr::EventId::all_zeros());
    assert!(Delivery::begin(&r, invalid, &f.keys, f.reserve())
        .await
        .is_err());
    let other = Fixture::new();
    let d = Delivery::begin(&r, other.dest.clone(), &other.keys, other.reserve())
        .await
        .unwrap();
    assert_eq!(
        d.finish(&r, &Keys::generate(), Ok("answer".into())).await,
        Outcome::Failed("signer_changed")
    );
    assert!(r.sent.lock().unwrap().is_empty());
}

#[cfg(unix)]
#[test]
fn journal_rejects_links_and_public_metadata() {
    use std::os::unix::fs::{symlink, PermissionsExt};
    let f = Fixture::new();
    drop(f.reserve());
    let metadata = std::fs::metadata(&f.dir).unwrap();
    assert_eq!(metadata.permissions().mode() & 0o777, 0o700);
    for item in std::fs::read_dir(&f.dir).unwrap() {
        assert_eq!(
            item.unwrap().metadata().unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    std::fs::set_permissions(&f.dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(Reservation::acquire(&f.dir, "new-batch").is_err());
    std::fs::set_permissions(&f.dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    let linked = f.dir.with_extension("link");
    symlink(&f.dir, &linked).unwrap();
    assert!(Reservation::acquire(&linked, "new-batch").is_err());
    std::fs::remove_file(linked).unwrap();
}
