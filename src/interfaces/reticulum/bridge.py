#!/usr/bin/env python3
import json
import os
import stat
import sys
import tempfile
import threading
import time

MAX_LINE_BYTES = 1024 * 1024
MAX_TEXT_BYTES = 256 * 1024
HASH_CHARS = 32
OUTPUT_LOCK = threading.Lock()


def emit(event):
    encoded = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
    if len(encoded.encode("utf-8")) + 1 > MAX_LINE_BYTES:
        raise ValueError("protocol event is too large")
    with OUTPUT_LOCK:
        sys.stdout.write(encoded + "\n")
        sys.stdout.flush()


def read_command(stream):
    line = stream.buffer.readline(MAX_LINE_BYTES + 1)
    if not line:
        return None
    if len(line) > MAX_LINE_BYTES or not line.endswith(b"\n"):
        raise ValueError("invalid protocol line")
    command = json.loads(line)
    if not isinstance(command, dict) or not isinstance(command.get("type"), str):
        raise ValueError("invalid protocol command")
    return command


def self_check():
    import io

    class FakeIdentity:
        remembered = None

        @staticmethod
        def remember(packet_hash, destination_hash, public_key):
            FakeIdentity.remembered = (packet_hash, destination_hash, public_key)

    class FakeDestination:
        @staticmethod
        def hash_from_name_and_identity(name, identity):
            assert name == "lxmf.delivery"
            assert identity == "identity"
            return b"d" * 16

    class FakeRns:
        Identity = FakeIdentity
        Destination = FakeDestination

    class FakeLink:
        link_id = b"l" * 16

    class UnknownMessage:
        unverified_reason = 1
        source_hash = b"s" * 16
        packed = b"packed"

    class FakeTransport:
        requested = None

        @staticmethod
        def request_path(destination_hash):
            FakeTransport.requested = destination_hash

    class RecoveringIdentity:
        calls = 0

        @staticmethod
        def recall(destination_hash, _no_use=False):
            RecoveringIdentity.calls += 1
            return "identity" if RecoveringIdentity.calls > 1 else None

    class RecoveringRns:
        Identity = RecoveringIdentity
        Transport = FakeTransport

    class RecoveringLxMessage:
        SOURCE_UNKNOWN = 1

        @staticmethod
        def unpack_from_bytes(packed):
            assert packed == b"packed"
            return "verified-message"

    class RecoveringLxmf:
        LXMessage = RecoveringLxMessage

    class Input:
        buffer = io.BytesIO(
            b'{"type":"start"}\n'
            b'{"type":"send","delivery_id":"one"}\n'
            b'{"type":"shutdown"}\n'
        )

    assert [read_command(Input())["type"] for _ in range(3)] == [
        "start",
        "send",
        "shutdown",
    ]
    assert validate_send({"delivery_id": "one", "destination": "b" * 32, "text": "hi"}) is None
    assert validate_send({"delivery_id": "one", "destination": "bad", "text": "hi"}) == "terminal"
    assert validate_send({"delivery_id": "one", "destination": "b" * 32, "text": " "}) == "terminal"
    assert inbound_is_text(
        signature_validated=True,
        text="hello",
        title="",
        fields={0x0F: [1, b"ticket"]},
        message_hash=b"a" * 32,
        source_hash=b"b" * 16,
    )
    assert inbound_rejection_reason(
        False, "hello", "", {}, b"a" * 32, b"b" * 16
    ) == "signature_unverified"
    remember_link_identity(FakeRns, FakeLink(), "identity", b"public-key")
    assert FakeIdentity.remembered == (b"l" * 16, b"d" * 16, b"public-key")
    assert recover_unknown_source(
        RecoveringRns,
        RecoveringLxmf,
        UnknownMessage(),
        deadline=lambda: False,
        sleep=lambda _: None,
    ) == "verified-message"
    assert FakeTransport.requested == b"s" * 16
    print("reticulum bridge self-check passed")


def ensure_directory(path):
    if os.path.lexists(path):
        metadata = os.lstat(path)
        if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
            raise ValueError(f"unsafe directory: {path}")
        if stat.S_IMODE(metadata.st_mode) != 0o700:
            raise ValueError(f"directory must have mode 0700: {path}")
        if metadata.st_uid != os.geteuid():
            raise ValueError(f"directory has unsafe owner: {path}")
    else:
        os.makedirs(path, mode=0o700)


def validate_regular_file(path):
    metadata = os.lstat(path)
    if not stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise ValueError(f"unsafe file: {path}")
    if stat.S_IMODE(metadata.st_mode) != 0o600:
        raise ValueError(f"file must have mode 0600: {path}")
    if metadata.st_uid != os.geteuid():
        raise ValueError(f"file has unsafe owner: {path}")


def write_private(path, data):
    directory = os.path.dirname(path)
    descriptor, temporary = tempfile.mkstemp(prefix=".codrik-", dir=directory)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def validate_hash(value):
    return (
        isinstance(value, str)
        and len(value) == HASH_CHARS
        and all(character in "0123456789abcdef" for character in value)
    )


def validate_send(command):
    destination = command.get("destination")
    text = command.get("text")
    if not validate_hash(destination) or not isinstance(text, str):
        return "terminal"
    if not text.strip() or len(text.encode("utf-8")) > MAX_TEXT_BYTES:
        return "terminal"
    return None


def inbound_is_text(signature_validated, text, title, fields, message_hash, source_hash):
    return inbound_rejection_reason(
        signature_validated, text, title, fields, message_hash, source_hash
    ) is None


def inbound_rejection_reason(
    signature_validated, text, title, fields, message_hash, source_hash
):
    if not signature_validated:
        return "signature_unverified"
    if text is None or not text.strip():
        return "empty_text"
    if len(text.encode("utf-8")) > MAX_TEXT_BYTES:
        return "text_too_large"
    if title:
        return "title_not_supported"
    if not isinstance(fields, dict):
        return "invalid_fields"
    if len(message_hash) != 32 or len(source_hash) != 16:
        return "invalid_hash"
    return None


def remember_link_identity(rns, link, identity, public_key):
    destination_hash = rns.Destination.hash_from_name_and_identity(
        "lxmf.delivery", identity
    )
    rns.Identity.remember(link.link_id, destination_hash, public_key)


def recover_unknown_source(rns, lxmf, message, deadline, sleep):
    if message.unverified_reason != lxmf.LXMessage.SOURCE_UNKNOWN:
        return message
    rns.Transport.request_path(message.source_hash)
    while rns.Identity.recall(message.source_hash, _no_use=True) is None:
        if deadline():
            return message
        sleep(0.1)
    return lxmf.LXMessage.unpack_from_bytes(message.packed)


def run():
    os.umask(0o077)
    start = read_command(sys.stdin)
    if start is None or set(start) != {"type", "state_dir", "rns_host", "rns_port"}:
        raise ValueError("first command must be start")
    if start["type"] != "start":
        raise ValueError("first command must be start")
    state_dir = start["state_dir"]
    host = start["rns_host"]
    port = start["rns_port"]
    if not os.path.isabs(state_dir):
        raise ValueError("state_dir must be absolute")
    if not isinstance(host, str) or not host or not all(
        character.isascii() and (character.isalnum() or character in ".-")
        for character in host
    ):
        raise ValueError("invalid RNS host")
    if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
        raise ValueError("invalid RNS port")

    try:
        import LXMF
        import RNS
    except Exception as error:
        emit({"type": "fatal", "error": f"failed to import RNS/LXMF: {error}"})
        return 1

    ensure_directory(state_dir)
    rns_dir = os.path.join(state_dir, "rns")
    ensure_directory(rns_dir)
    config_path = os.path.join(rns_dir, "config")
    if os.path.lexists(config_path):
        validate_regular_file(config_path)
    config = f"""[reticulum]
  share_instance = No

[interfaces]
  [[Codrik TCP Client]]
    type = TCPClientInterface
    enabled = Yes
    target_host = {host}
    target_port = {port}
    mode = full
""".encode("ascii")
    write_private(config_path, config)

    identity_path = os.path.join(state_dir, "identity")
    if os.path.lexists(identity_path):
        validate_regular_file(identity_path)
        identity = RNS.Identity.from_file(identity_path)
        if identity is None:
            raise ValueError("failed to load Reticulum identity")
    else:
        identity = RNS.Identity()
        temporary = identity_path + f".tmp-{os.getpid()}"
        identity.to_file(temporary)
        os.chmod(temporary, 0o600)
        os.replace(temporary, identity_path)

    RNS.Reticulum(configdir=rns_dir)
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        interfaces = list(getattr(RNS.Transport, "interfaces", []))
        if any(
            "Codrik TCP Client" in str(interface)
            and bool(getattr(interface, "online", False))
            for interface in interfaces
        ):
            break
        time.sleep(0.1)
    else:
        raise ConnectionError("Reticulum TCP interface did not become online")

    router = LXMF.LXMRouter(storagepath=state_dir)
    source = router.register_delivery_identity(identity, display_name="Codrik")
    if source is None:
        raise ValueError("failed to register LXMF identity")

    def inbound(message):
        recovery_deadline = time.monotonic() + 7
        message = recover_unknown_source(
            RNS,
            LXMF,
            message,
            deadline=lambda: time.monotonic() >= recovery_deadline,
            sleep=time.sleep,
        )
        text = message.content_as_string()
        rejection = inbound_rejection_reason(
            message.signature_validated,
            text,
            message.title_as_string(),
            message.fields,
            message.hash,
            message.source_hash,
        )
        if rejection is not None:
            print(f"inbound LXMF rejected: {rejection}", file=sys.stderr, flush=True)
            return
        emit(
            {
                "type": "inbound",
                "message_hash": message.hash.hex(),
                "source": message.source_hash.hex(),
                "timestamp": message.timestamp,
                "text": text,
            }
        )

    router.register_delivery_callback(inbound)
    original_remote_identified = router.delivery_remote_identified

    def remote_identified(link, remote_identity):
        original_remote_identified(link, remote_identity)
        remember_link_identity(
            RNS, link, remote_identity, remote_identity.get_public_key()
        )

    source.set_link_established_callback(
        lambda link: (
            router.delivery_link_established(link),
            link.set_remote_identified_callback(remote_identified),
        )
    )
    router.announce(source.hash)
    emit({"type": "ready", "destination": source.hash.hex()})
    active = set()

    def delivery(delivery_id, outcome, retry_after_ms=None):
        if delivery_id not in active:
            return
        active.remove(delivery_id)
        event = {"type": "delivery", "delivery_id": delivery_id, "outcome": outcome}
        if retry_after_ms is not None:
            event["retry_after_ms"] = retry_after_ms
        emit(event)

    def send(command):
        delivery_id = command.get("delivery_id")
        destination_hex = command.get("destination")
        text = command.get("text")
        if not isinstance(delivery_id, str) or not delivery_id or delivery_id in active:
            raise ValueError("invalid or duplicate delivery ID")
        invalid = validate_send(command)
        active.add(delivery_id)
        if invalid is not None:
            delivery(delivery_id, invalid)
            return
        destination_hash = bytes.fromhex(destination_hex)
        if not RNS.Transport.has_path(destination_hash):
            RNS.Transport.request_path(destination_hash)
            deadline = time.monotonic() + 15
            while not RNS.Transport.has_path(destination_hash) and time.monotonic() < deadline:
                time.sleep(0.1)
        recipient = RNS.Identity.recall(destination_hash)
        if recipient is None:
            delivery(delivery_id, "retryable")
            return
        destination = RNS.Destination(
            recipient,
            RNS.Destination.OUT,
            RNS.Destination.SINGLE,
            "lxmf",
            "delivery",
        )
        message = LXMF.LXMessage(
            destination,
            source,
            text,
            desired_method=LXMF.LXMessage.DIRECT,
            include_ticket=True,
        )
        message.register_delivery_callback(lambda _: delivery(delivery_id, "delivered"))
        message.register_failed_callback(
            lambda failed: delivery(
                delivery_id,
                "terminal"
                if failed.state == LXMF.LXMessage.REJECTED
                else "retryable",
            )
        )
        try:
            router.handle_outbound(message)
        except Exception:
            delivery(delivery_id, "retryable")

    while True:
        command = read_command(sys.stdin)
        if command is None or command.get("type") == "shutdown":
            return 0
        if command.get("type") != "send" or set(command) != {
            "type",
            "delivery_id",
            "destination",
            "text",
        }:
            raise ValueError("unknown protocol command")
        send(command)


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-check"]:
        self_check()
        raise SystemExit(0)
    if sys.argv[1:]:
        raise SystemExit("bridge accepts no arguments")
    try:
        raise SystemExit(run())
    except Exception as error:
        try:
            emit({"type": "fatal", "error": str(error)[:4096] or "bridge failed"})
        except Exception:
            pass
        raise SystemExit(1)
