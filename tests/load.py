#!/usr/bin/env python3
"""Integrity, memory and limit tests for machin-rooms (Linux, Python 3 stdlib only).

    ./build.sh && python3 tests/load.py            # full run, a few minutes
    python3 tests/load.py --quick                  # about a tenth of the load

It starts its own server on a free loopback port with a 8 GiB address-space cap (thread stacks reserve a lot of it),
and stops only that process. Exit status 0 = every check passed.
"""
import hashlib, json, os, random, resource, socket, struct, subprocess, sys, threading, time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.path.join(ROOT, "machin-rooms")
QUICK = "--quick" in sys.argv
SCALE = 10 if QUICK else 1


def check(cond, what):
    if not cond:
        raise AssertionError(what)


class Client:
    def __init__(self, port, room, name, rcvbuf=0):
        self.s = socket.socket()
        if rcvbuf:
            self.s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
        self.s.settimeout(60)
        self.s.connect(("127.0.0.1", port))
        self.s.sendall((f"GET /ws?room={room}&name={name} HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\n"
                        "Connection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                        "Sec-WebSocket-Version: 13\r\n\r\n").encode())
        self.f = self.s.makefile("rb")
        check(b"101" in self.f.readline(), "no upgrade")
        while self.f.readline() not in (b"\r\n", b""):
            pass

    def send(self, data):
        n = len(data)
        if n < 126:
            h = bytes([129, 128 | n])
        elif n < 65536:
            h = bytes([129, 254]) + struct.pack(">H", n)
        else:
            h = bytes([129, 255]) + struct.pack(">Q", n)
        self.s.sendall(h + b"\0\0\0\0" + data)

    def recv(self):
        """The next JSON message, or None once the server has closed."""
        h = self.f.read(2)
        if len(h) < 2 or h[0] & 15 == 8:
            return None
        check(h[0] == 129 and not h[1] & 128, f"bad frame header {h!r}")
        n = h[1]
        if n == 126:
            n = struct.unpack(">H", self.f.read(2))[0]
        elif n == 127:
            n = struct.unpack(">Q", self.f.read(8))[0]
        p = self.f.read(n)
        check(len(p) == n, "truncated frame")
        return json.loads(p.decode())

    def until(self, pred):
        while True:
            m = self.recv()
            check(m is not None, "closed while waiting")
            if pred(m):
                return m

    def close(self):
        try:
            self.s.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.f.close()
        self.s.close()


def joined(port, room, name, **kw):
    c = Client(port, room, name, **kw)
    c.until(lambda m: m["type"] == "sys" and m["text"] == name + " joined")
    return c


def text_for(room, name, seq):
    """A reproducible message: size and content depend on who sent it and when."""
    r = random.Random(f"{room}/{name}/{seq}")
    size = r.choice([20, 125, 126, 300, 2000, 10000, 10000, 10000, 65535, 65536])
    if seq == 7:
        size = 100000
    unit = hashlib.sha256(f"{room}/{name}/{seq}".encode()).hexdigest() + ' "q" \\ \n é€ <&> '
    body = (unit * (size // len(unit) + 1))[:size]
    return f"{seq}|{body}|end"


def integrity(port, rooms=4, members=5, count=120 // (4 if QUICK else 1)):
    """Concurrent senders in several rooms while other clients churn: each member
    must get every message of its room, exact, once, in per-sender order."""
    everyone = []
    for r in range(rooms):
        for m in range(members):
            everyone.append((f"int{r}", f"m{r}-{m}", joined(port, f"int{r}", f"m{r}-{m}")))
    errors, stop = [], threading.Event()
    total = [0]

    def member(room, name, c):
        window = threading.Semaphore(3)          # at most 3 of our messages in flight

        def sender():
            for i in range(count):
                if not window.acquire(timeout=60):
                    return
                c.send(text_for(room, name, i).encode())

        t = threading.Thread(target=sender, daemon=True)
        t.start()
        try:
            nxt = {}
            left = members * count
            while left:
                m = c.recv()
                check(m is not None, f"{name} was disconnected")
                check(m["room"] == room, f"{name} got a message of room {m['room']}")
                if m["type"] != "msg":
                    continue
                who, i = m["name"], nxt.get(m["name"], 0)
                check(who.startswith("m" + room[3:] + "-"), f"foreign sender {who}")
                check(m["text"] == text_for(room, who, i), f"{name}: message {i} from {who} is wrong")
                nxt[who] = i + 1
                left -= 1
                total[0] += 1
                if who == name:
                    window.release()
            t.join()
        except Exception as e:
            errors.append(f"{name}: {e!r}")

    def churner(k):
        i = 0
        while not stop.is_set():
            try:
                c = joined(port, f"int{i % rooms}x{k}", f"churn{k}-{i}")
                c.send(os.urandom(5000).hex().encode())
                if i % 2:
                    c.until(lambda m: m["type"] == "msg")
                c.close()                         # half leave with an echo still on the way
            except Exception as e:
                errors.append(f"churn: {e!r}")
                return
            i += 1

    ts = [threading.Thread(target=member, args=e) for e in everyone]
    cs = [threading.Thread(target=churner, args=(k,)) for k in range(3)]
    for t in ts + cs:
        t.start()
    for t in ts:
        t.join()
    stop.set()
    for t in cs:
        t.join()
    check(not errors, errors[:5])
    check(total[0] == rooms * members * members * count, f"only {total[0]} deliveries")
    for _, _, c in everyone:
        c.close()
    print(f"integrity: {total[0]} deliveries verified byte for byte across {rooms} rooms, with churn")


def sustained(port, pid):
    rss = lambda: int([l for l in open(f"/proc/{pid}/status") if l.startswith("VmRSS")][0].split()[1])
    samples = []

    def sample(label):
        time.sleep(0.5)
        samples.append(rss())
        print(f"  {label:44s} RSS {samples[-1]:7d} kB", flush=True)

    # idle members stay connected, so every hub handover carries a member list
    idle = [joined(port, f"idle{i}", f"idle{i}") for i in range(50)]
    a, b = joined(port, "load", "a"), joined(port, "load", "b")
    sample("start (52 members)")
    per, n = 10000 // SCALE, 0
    for block in range(6):
        for _ in range(per):
            text = f"{n:010d}" + "x" * 9990
            a.send(text.encode())
            for c in (a, b):
                got = c.until(lambda m: m["type"] == "msg")
                check(got["text"] == text and got["name"] == "a", f"message {n} corrupted")
            n += 1
        sample(f"{n} messages of 10 kB")
    msg_samples = samples[1:]
    first = len(samples)
    k = 0
    for block in range(8):
        for _ in range(1000 // SCALE):
            room, name = f"room{k}-" + "r" * 40, f"user{k}"
            c = joined(port, room, name)
            text = f"{k:010d}" + "y" * 9990
            c.send(text.encode())
            check(c.until(lambda m: m["type"] == "msg")["text"] == text, f"churn {k} corrupted")
            c.close()
            k += 1
        sample(f"{k} connect/10 kB/disconnect, new room+name")
    churn_samples = samples[first:]
    for i, c in enumerate(idle):                  # the member list survived every handover
        c.send(f"still here {i}".encode())
        got = c.until(lambda m: m["type"] == "msg")
        check(got["room"] == f"idle{i}" and got["text"] == f"still here {i}", "idle member lost")
    for c in idle + [a, b]:
        c.close()
    if not QUICK:
        for label, s in (("messages", msg_samples), ("churn", churn_samples)):
            grew = max(s[len(s) // 2:]) - max(s[:len(s) // 2])
            check(grew < 4096, f"no plateau in {label}: later half is {grew} kB higher")
    print(f"sustained: {n} messages + {k} connect cycles intact")
    return rss


def limits(port, pid, rss):
    c = Client(port, "R" * 5000, "N" * 5000)
    m = c.recv()
    check(len(m["room"]) == 64 and m["text"] == "N" * 32 + " joined", "room/name not clipped")
    c.close()
    c = Client(port, "u", "%C3%A9" * 40)
    check(c.recv()["text"] == "é" * 16 + " joined", "clip split a UTF-8 character")
    c.close()
    c = joined(port, "big", "big")
    c.send(b"z" * 131072)
    check(len(c.until(lambda m: m["type"] == "msg")["text"]) == 131072, "128 KiB message refused")
    try:
        c.send(b"z" * 131073)
        check(c.recv() is None, "oversized message accepted")
    except OSError:
        pass
    c.close()

    before = rss()
    slow = joined(port, "slow", "slow", rcvbuf=4096)          # never reads again
    fast = joined(port, "slow", "fast")
    left = False
    for i in range(3000):
        fast.send(b"s" * 10000)
        while True:
            m = fast.recv()
            check(m is not None, "the reading member was disconnected")
            if m["type"] == "msg":
                break
            left = left or m["text"] == "slow left"
        peak = rss()
        check(peak - before < 40000, f"backlog grew to {peak - before} kB")
    check(left, "the member that never reads was not disconnected")
    fast.close()
    slow.close()
    print(f"limits: clipping, message cap, slow reader dropped (RSS +{rss() - before} kB over 30 MB)")

    # a member that stops reading, then closes only its sending side: both threads must end
    threads = lambda: int(open(f"/proc/{pid}/status").read().split("Threads:")[1].split()[0])
    time.sleep(1)
    idle_threads = threads()
    half = joined(port, "half", "half", rcvbuf=4096)
    other = joined(port, "half", "other")
    for _ in range(500):                                      # about 5 MB: under the backlog cap
        other.send(b"h" * 10000)
        other.until(lambda m: m["type"] == "msg")
    half.s.shutdown(socket.SHUT_WR)
    other.until(lambda m: m["text"] == "half left")
    other.close()
    time.sleep(2)
    check(threads() <= idle_threads, f"{threads() - idle_threads} threads stuck after a half-close")
    half.close()
    print("limits: half-closed non-reader is fully released")

    before = rss()
    s = socket.create_connection(("127.0.0.1", port), timeout=10)
    peak = before
    try:
        s.sendall(b"POST / HTTP/1.1\r\nContent-Length: 2000000000\r\n\r\n")
        for _ in range(160):
            s.sendall(b"y" * 65536)
            peak = max(peak, rss())
    except OSError:
        pass
    s.close()
    check(peak - before < 4096, f"oversized HTTP body buffered ({peak - before} kB)")
    print("limits: oversized HTTP body is not buffered")


def main():
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    srv = subprocess.Popen([BIN], env=dict(os.environ, MACHROOMS_PORT=str(port)), stdout=subprocess.DEVNULL,
                           preexec_fn=lambda: resource.setrlimit(resource.RLIMIT_AS, (8 << 30, 8 << 30)))
    try:
        for _ in range(100):
            try:
                socket.create_connection(("127.0.0.1", port)).close()
                break
            except OSError:
                time.sleep(0.05)
        integrity(port)
        rss = sustained(port, srv.pid)
        integrity(port)
        limits(port, srv.pid, rss)
        check(srv.poll() is None, "server exited")
        print("PASS")
    finally:
        srv.kill()
        srv.wait()


main()
