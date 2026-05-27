# AIOPE Personal AI Host — Full Spec

## Purpose

Dedicated host for aiope-headless (standalone branch) — a personal AI assistant that mimics personality and thinking patterns, with access to personal information, embedding-based vector memory, and a fine-tuned model (future).

---

## Infrastructure

| Parameter | Value |
|---|---|
| Provider | AWS EC2 |
| Region | eu-central-2 (Zurich, Switzerland) |
| Instance | m6i.large |
| vCPU | 2 (Intel Xeon 8375C Ice Lake, 3.5 GHz sustained) |
| RAM | 8 GiB |
| Architecture | x86_64 (AMD64v3) |
| Storage | gp3 EBS, 30 GB, 3000 IOPS / 125 MB/s baseline |
| OS | Ubuntu 26.04 LTS (Resolute Raccoon) |
| AMI | ami-04a6af3b79f89c04b |
| Hibernation | Enabled |
| Cost | ~$92/mo on-demand (+ ~$2.40/mo EBS) |

### Why Zurich
- Swiss Federal Act on Data Protection (FADP) — strongest privacy law globally
- No US CLOUD Act jurisdiction over data at rest
- Personal data (conversations, photos metadata, embeddings) stays in Swiss legal territory

### Why m6i.large
- Highest sustained clock (3.5 GHz) in the price range
- AVX-512 for embedding model inference (llama.cpp / GGUF)
- 8 GiB RAM fits: OS (~0.5G) + aiope-headless (~0.1G) + embedding model (~1G) + vector DB (~2-3G) + headroom
- 2 cores allow CPU pinning: agent on core 0, embeddings on core 1

---

## Software Stack

| Component | Role |
|---|---|
| aiope-headless (Go) | AI agent runtime — chat, tools, WebSocket, SQLite |
| llama-server (nano) | Real-time embedding queries (always-on) |
| llama-server (small) | Batch ingestion / backfill (on-demand) |
| Vector DB (sqlite-vec) | Personal knowledge retrieval — same DB file as chat |
| Docker (optional) | Sandboxed tool execution |

### Embedding Models

Dual-model architecture: nano for real-time, small for batch/quality.

| | Nano (real-time) | Small (batch/backfill) |
|---|---|---|
| Model | jina-embeddings-v5-text-nano | jina-embeddings-v5-text-small |
| Params | 239M | 677M |
| MTEB English | 71.0 | 71.7 |
| Context | 8K tokens | 32K tokens |
| Dims | 768 (Matryoshka: 32-768) | 1024 (Matryoshka: 32-1024) |
| Quant | Q4_K_M (~150MB) | Q4_K_M (~500MB) |
| RAM loaded | ~300MB | ~500MB |
| Use case | Every incoming query, real-time similarity | Ingest new docs, re-index, long documents |
| Scheduling | Always-on, SCHED_FIFO | On-demand / cron, SCHED_BATCH |
| License | CC BY-NC 4.0 (personal use OK) | CC BY-NC 4.0 (personal use OK) |

**Strategy:**
- Nano handles every message embedding with sub-50ms latency
- Small runs for batch ingestion (new personal data, photos, conversations) and re-indexing
- Both produce 768d vectors (small truncated via Matryoshka) for a unified index
- Small's 32K context embeds full documents without chunking

---

## Kernel

### Base
```
linux-lowlatency (Ubuntu 26.04 default repo)
```
Preemptive scheduling model — agent responses never blocked by embedding batch work.

### Boot Parameters
```
GRUB_CMDLINE_LINUX="mitigations=off nmi_watchdog=0 nowatchdog idle=poll intel_idle.max_cstate=1 processor.max_cstate=1 tsx=on transparent_hugepage=always"
```

| Param | Effect |
|---|---|
| mitigations=off | 5-15% perf gain, safe on single-tenant |
| nmi_watchdog=0 | Eliminate 1 interrupt/sec/core |
| idle=poll / max_cstate=1 | Instant CPU wake, no C-state latency |
| tsx=on | Transactional memory for lock-heavy SQLite |
| transparent_hugepage=always | Fewer TLB misses for model mmap |

---

## System Tuning

### CPU Scheduler
```ini
# /etc/sysctl.d/99-sched.conf
kernel.sched_min_granularity_ns = 1000000
kernel.sched_wakeup_granularity_ns = 500000
kernel.sched_migration_cost_ns = 250000
kernel.sched_autogroup_enabled = 0
```

### Memory
```ini
# /etc/sysctl.d/99-vm.conf
vm.swappiness = 10
vm.dirty_ratio = 40
vm.dirty_background_ratio = 5
vm.dirty_expire_centisecs = 3000
vm.dirty_writeback_centisecs = 500
vm.vfs_cache_pressure = 50
vm.zone_reclaim_mode = 0
vm.compact_memory = 1
```

### Network
```ini
# /etc/sysctl.d/99-net.conf
net.core.default_qdisc = fq
net.ipv4.tcp_congestion_control = bbr
net.core.somaxconn = 4096
net.core.netdev_max_backlog = 5000
net.ipv4.tcp_max_syn_backlog = 4096
net.ipv4.tcp_fastopen = 3
net.ipv4.tcp_slow_start_after_idle = 0
```

### Security / Memory Layout
```ini
# /etc/sysctl.d/99-sec.conf
kernel.randomize_va_space = 1
```

---

## I/O & Storage

### Scheduler
```ini
# /etc/udev/rules.d/60-scheduler.conf
ACTION=="add|change", KERNEL=="nvme*", ATTR{queue/scheduler}="none"
ACTION=="add|change", KERNEL=="xvd*", ATTR{queue/scheduler}="none"
```

### Block Device
```bash
echo 256 > /sys/block/nvme0n1/queue/nr_requests
echo 2048 > /sys/block/nvme0n1/queue/read_ahead_kb
echo 0 > /sys/block/nvme0n1/queue/iostats
echo 0 > /sys/block/nvme0n1/queue/wbt_lat_usec
```

### Filesystem (ext4)
```
UUID=xxx / ext4 noatime,commit=60,discard,errors=remount-ro 0 1
```

### ZRAM (no EBS swap)
```ini
# /etc/systemd/zram-generator.conf
[zram0]
zram-size = ram * 2
compression-algorithm = zstd
```

---

## NIC Tuning (ENA)

```bash
ethtool -G ens5 rx 8192 tx 8192
ethtool -K ens5 gro on gso on tso on
ethtool -C ens5 adaptive-rx on adaptive-tx on
```

---

## Interrupt Affinity

```bash
# Pin NIC + NVMe interrupts to CPU 0
# Keep CPU 1 clean for embedding compute
echo 0 > /proc/irq/<nic_irq>/smp_affinity_list
echo 0 > /proc/irq/<nvme_irq>/smp_affinity_list
```

---

## Process Isolation (cgroup v2)

### aiope-headless.service
```ini
[Service]
ExecStart=/usr/local/bin/aiope-headless
CPUAffinity=0
CPUSchedulingPolicy=fifo
CPUSchedulingPriority=10
MemoryMax=2G
IOWeight=200
Restart=always
```

### embedding-nano.service (always-on, real-time queries)
```ini
[Service]
ExecStart=/usr/local/bin/llama-server \
  -hf jinaai/jina-embeddings-v5-text-nano-retrieval-GGUF:Q4_K_M \
  --embedding --pooling last --port 8091 -ub 8192
CPUAffinity=0
CPUSchedulingPolicy=fifo
CPUSchedulingPriority=5
MemoryMax=1G
IOWeight=150
Restart=always
```

### embedding-small.service (on-demand, batch ingestion)
```ini
[Service]
ExecStart=/usr/local/bin/llama-server \
  -hf jinaai/jina-embeddings-v5-text-small-retrieval-GGUF:Q4_K_M \
  --embedding --pooling last --port 8092 -ub 32768
CPUAffinity=1
CPUSchedulingPolicy=batch
MemoryMax=5G
IOWeight=50
Type=oneshot
RemainAfterExit=no
```

---

## Kernel Memory

```bash
# Disable KSM (no VMs, wastes CPU)
echo 0 > /sys/kernel/mm/ksm/run

# THP already enabled via boot param, set defrag policy
echo defer+madvise > /sys/kernel/mm/transparent_hugepage/defrag
```

---

## Hibernation

- Root EBS volume: 30 GB gp3 (≥ 8 GB free for RAM dump)
- Encrypted EBS with NitroTPM attestation
- Max hibernate duration: 60 days
- Resume time: ~20-30 seconds
- Client reconnect required after wake

### Cost when hibernated
- EBS only: 30 GB × $0.0952/GB = ~$2.86/mo
- No compute charges

---

## Data Layout

```
/
├── /data/
│   ├── aiope2-chat.db          # SQLite — conversations
│   ├── vectors/                 # Vector DB index (768d unified)
│   ├── models/
│   │   ├── jina-v5-nano-Q4_K_M.gguf    # ~150MB, real-time
│   │   └── jina-v5-small-Q4_K_M.gguf   # ~500MB, batch
│   ├── uploads/                 # Attached images
│   ├── generated/               # AI-generated content
│   ├── personal/                # Personal documents for RAG
│   └── workspace/               # Agent scratch area
├── /usr/local/bin/
│   ├── aiope-headless
│   └── llama-server
└── /etc/systemd/system/
    ├── aiope-headless.service
    ├── embedding-nano.service
    └── embedding-small.service
```

---

## Network / Access

| Method | Details |
|---|---|
| SSH | Ed25519 key, port 22 (or custom) |
| Web UI | aiope-headless on port 8090 |
| Embedding (nano) | llama-server on port 8091 (localhost only, real-time) |
| Embedding (small) | llama-server on port 8092 (localhost only, batch) |
| Firewall | UFW: allow SSH + 8090 from personal IPs only |

---

## Future Additions

| When | What |
|---|---|
| Fine-tuned model ready | Add GPU instance (g4dn.xlarge, start/stop on demand) or serve via gateway |
| Photo training complete | Ingest photo metadata + captions into vector DB |
| Conversation model trained | Point aiope-headless provider at self-hosted fine-tuned model |
| Scale needed | Upgrade to m6i.xlarge (4 vCPU, 16 GiB) — same optimizations apply |

---

## Estimated Monthly Cost

| Component | Cost |
|---|---|
| m6i.large (always-on) | $92.00 |
| EBS gp3 30GB | $2.86 |
| Data transfer (light) | ~$2.00 |
| **Total** | **~$97/mo** |

With hibernation (12hr active / 12hr sleep):
| Component | Cost |
|---|---|
| m6i.large (50% uptime) | $46.00 |
| EBS gp3 30GB (always) | $2.86 |
| **Total** | **~$49/mo** |
