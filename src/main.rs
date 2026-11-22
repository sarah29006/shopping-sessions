//! Shopping Sessions: native single-binary pipeline implementing rules.md.
//!
//! Phases
//!  1. mmap every input; read manifest, products, campaigns.
//!  2. links -> union-find -> open-addressing table user-hash -> person key.
//!  3. parse events (records partitioned by event_id hash) and clicks
//!     (valid only, partitioned by person) in parallel, with an AVX2 tokenizer.
//!  4. per event_id partition: dedup (smallest ingest_ts), keep valid copies,
//!     re-partition by person.
//!  5. per person partition: sort, sessionize, credit + assist, aggregate.
//!  6. write both Parquet tables.
//!
//! Strings are never copied: a "ref" packs (file, offset, length) of the
//! unquoted bytes inside the memory-mapped input.

use std::fs::File;
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering as AO};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use memchr::memchr;
use memmap2::Mmap;
use xxhash_rust::xxh3::{xxh3_64, xxh3_128};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const DAY_MS: i64 = 86_400_000;
const GAP_MS: u32 = 1_800_000;
const WEEK_MS: i64 = 7 * DAY_MS;

const P1_BITS: u32 = 9; // event_id partitions
const P1: usize = 1 << P1_BITS;
const P2_BITS: u32 = 9; // person partitions
const P2: usize = 1 << P2_BITS;

const CHUNK: usize = 2 << 20;

// ------------------------------------------------------------------ files

static FILES: OnceLock<Vec<Option<Mmap>>> = OnceLock::new();

#[inline]
fn fdata(i: usize) -> &'static [u8] {
    match &FILES.get().unwrap()[i] {
        Some(m) => &m[..],
        None => &[],
    }
}

const OFF_BITS: u32 = 38;
const LEN_BITS: u32 = 14;

#[inline]
fn mkref(file: usize, off: usize, len: usize) -> u64 {
    let len = len.min((1 << LEN_BITS) - 1);
    ((file as u64) << (OFF_BITS + LEN_BITS)) | ((len as u64) << OFF_BITS) | off as u64
}

#[inline]
fn rbytes(r: u64) -> &'static [u8] {
    let file = (r >> (OFF_BITS + LEN_BITS)) as usize;
    let len = ((r >> OFF_BITS) & ((1 << LEN_BITS) - 1)) as usize;
    let off = (r & ((1 << OFF_BITS) - 1)) as usize;
    &fdata(file)[off..off + len]
}

// ------------------------------------------------------------------ hashing

#[derive(Default)]
struct IdHasher(u64);
impl Hasher for IdHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline]
    fn write(&mut self, _: &[u8]) {
        unreachable!()
    }
    #[inline]
    fn write_u64(&mut self, x: u64) {
        self.0 = x;
    }
}
type IdBuild = BuildHasherDefault<IdHasher>;
type IdMap<K, V> = hashbrown::HashMap<K, V, IdBuild>;

/// User-id hash; never 0 (0 marks an empty table slot).
#[inline]
fn uhash(b: &[u8]) -> u64 {
    let h = xxh3_64(b);
    if h == 0 { 1 } else { h }
}

// ------------------------------------------------------------------ prefetch

#[inline(always)]
fn prefetch<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch(p as *const i8, std::arch::x86_64::_MM_HINT_T0);
    }
}

/// Open-addressing table user-hash -> person key, for linked IDs only.
struct LinkTab {
    mask: usize,
    slots: Vec<[u64; 2]>, // (user hash, person key); key 0 = empty
    refs: Vec<u64>,       // ref of each slot's own ID
}

impl LinkTab {
    #[inline]
    fn slot(&self, h: u64) -> usize {
        (h as usize) & self.mask
    }
    /// Name ref of a linked person (its key is the hash of its smallest ID).
    fn name(&self, pk: u64) -> Option<u64> {
        let mut i = self.slot(pk);
        loop {
            let s = &self.slots[i];
            if s[0] == pk {
                return Some(self.refs[i]);
            }
            if s[0] == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline]
    fn pf(&self, h: u64) {
        prefetch(unsafe { self.slots.as_ptr().add(self.slot(h)) });
    }
    #[inline]
    fn person(&self, h: u64) -> u64 {
        let mut i = self.slot(h);
        loop {
            let s = unsafe { self.slots.get_unchecked(i) };
            if s[0] == h {
                return s[1];
            }
            if s[0] == 0 {
                return h;
            }
            i = (i + 1) & self.mask;
        }
    }
}

// ------------------------------------------------------------------ tokenizer

#[inline(always)]
fn masks64(p: *const u8) -> (u64, u64, u64) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use std::arch::x86_64::*;
        let a = _mm256_loadu_si256(p as *const __m256i);
        let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
        let m = |x: __m256i, c: u8| -> u32 { _mm256_movemask_epi8(_mm256_cmpeq_epi8(x, _mm256_set1_epi8(c as i8))) as u32 };
        let nl = m(a, b'\n') as u64 | (m(b, b'\n') as u64) << 32;
        let cm = m(a, b',') as u64 | (m(b, b',') as u64) << 32;
        let q = m(a, b'"') as u64 | (m(b, b'"') as u64) << 32;
        (nl, cm, q)
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    unsafe {
        let mut nl = 0u64;
        let mut cm = 0u64;
        let mut q = 0u64;
        for i in 0..64 {
            let c = *p.add(i);
            nl |= ((c == b'\n') as u64) << i;
            cm |= ((c == b',') as u64) << i;
            q |= ((c == b'"') as u64) << i;
        }
        (nl, cm, q)
    }
}

/// Calls `f(line_start, fields)` for every well-formed line in d[s..e] with
/// exactly N fields; fields are unquoted absolute (start, end) ranges.
#[inline(always)]
fn scan<const N: usize>(d: &'static [u8], s: usize, e: usize, mut f: impl FnMut(usize, &[(usize, usize); N])) {
    let mut fields = [(0usize, 0usize); N];
    let mut nf = 0usize;
    let mut line_start = s;
    let mut fstart = s;
    let mut fq = 0u32; // quote characters seen in the current field
    let mut bad = false;
    let mut pos = s;
    let mut buf = [0u8; 64];
    while pos < e {
        let n = e - pos;
        let (mnl, mc, mq) = if n >= 64 {
            masks64(unsafe { d.as_ptr().add(pos) })
        } else {
            buf[..n].copy_from_slice(&d[pos..e]);
            buf[n..].fill(0);
            masks64(buf.as_ptr())
        };
        let mut q = mq;
        let mut seps = mnl | mc;
        while seps != 0 {
            let b = seps.trailing_zeros();
            let p = pos + b as usize;
            let bit = 1u64 << b;
            let below = bit - 1;
            fq += (q & below).count_ones();
            q &= !(below | bit);
            let mut st = fstart;
            let mut en = p;
            if fq != 0 {
                if fq == 2 && en - st >= 2 && unsafe { *d.get_unchecked(st) == b'"' && *d.get_unchecked(en - 1) == b'"' } {
                    st += 1;
                    en -= 1;
                } else {
                    bad = true;
                }
                fq = 0;
            }
            if nf < N {
                fields[nf] = (st, en);
            }
            nf += 1;
            fstart = p + 1;
            if mnl & bit != 0 {
                if nf == N && !bad {
                    f(line_start, &fields);
                }
                nf = 0;
                bad = false;
                line_start = p + 1;
            }
            seps &= seps - 1;
        }
        fq += q.count_ones();
        pos += 64;
    }
    if line_start < e {
        let mut st = fstart;
        let mut en = e;
        if fq != 0 {
            if fq == 2 && en - st >= 2 && d[st] == b'"' && d[en - 1] == b'"' {
                st += 1;
                en -= 1;
            } else {
                bad = true;
            }
        }
        if nf < N {
            fields[nf] = (st, en);
        }
        nf += 1;
        if nf == N && !bad {
            f(line_start, &fields);
        }
    }
}

/// Byte ranges [start, end) of each chunk of a file's data lines.
fn chunks_of(file: usize, out: &mut Vec<(usize, usize, usize)>) {
    let d = fdata(file);
    let begin = match memchr(b'\n', d) {
        Some(i) => i + 1,
        None => return,
    };
    let mut s = begin;
    while s < d.len() {
        let mut e = (s + CHUNK).min(d.len());
        if e < d.len() {
            match memchr(b'\n', &d[e..]) {
                Some(i) => e = e + i + 1,
                None => e = d.len(),
            }
        }
        out.push((file, s, e));
        s = e;
    }
}

// ------------------------------------------------------------------ time

#[inline(always)]
fn dig2(a: u8, b: u8) -> Option<i64> {
    let x = a.wrapping_sub(b'0');
    let y = b.wrapping_sub(b'0');
    if x < 10 && y < 10 { Some((x * 10 + y) as i64) } else { None }
}

#[inline]
fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

/// F1 / F2 / F3 -> epoch ms, None when bad.
#[inline]
fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 0 {
        return None;
    }
    if n == 20 || n == 25 {
        if s[4] == b'-' {
            return parse_iso(s);
        }
    }
    if n <= 16 {
        let mut buf = [b'0'; 16];
        buf[16 - n..].copy_from_slice(s);
        let a = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let b = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        if !all_digits8(a) || !all_digits8(b) {
            return None;
        }
        return Some((parse8(a) * 100_000_000 + parse8(b)) as i64);
    }
    let mut v: i64 = 0;
    let mut sig = 0usize;
    for &c in s {
        let x = c.wrapping_sub(b'0');
        if x >= 10 {
            return None;
        }
        if sig > 0 || x != 0 {
            sig += 1;
            if sig > 18 {
                return None; // far outside any window
            }
            v = v * 10 + x as i64;
        }
    }
    Some(v)
}

#[inline(always)]
fn all_digits8(v: u64) -> bool {
    let x = v ^ 0x3030_3030_3030_3030; // digits -> 0..9
    let y = (x & 0x7F7F_7F7F_7F7F_7F7F) + 0x7676_7676_7676_7676; // high bit set iff low7 >= 10
    (x | y) & 0x8080_8080_8080_8080 == 0
}

/// 8 ASCII digits (little-endian load, most significant first) -> value.
#[inline(always)]
fn parse8(v: u64) -> u64 {
    let v = v - 0x3030_3030_3030_3030;
    let v = (v * 10) + (v >> 8);
    let v = ((v & 0x0000_00FF_0000_00FF).wrapping_mul(100 + (1_000_000 << 32))
        + ((v >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000 << 32)))
        >> 32;
    v
}

const CUM_DAYS: [i64; 13] = [0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];

#[inline]
fn parse_iso(s: &[u8]) -> Option<i64> {
    // "YYYY-MM-DDTHH:MM:SS" then "Z" or "+HH:MM"
    let a = u64::from_le_bytes(s[0..8].try_into().unwrap()); // YYYY-MM-
    let b = u64::from_le_bytes(s[8..16].try_into().unwrap()); // DDTHH:MM
    let c = u32::from_le_bytes(s[15..19].try_into().unwrap()); // M:SS
    // separators
    const SEP_A_MASK: u64 = 0xFF00_00FF_0000_0000;
    const SEP_A: u64 = 0x2D00_002D_0000_0000; // '-' at 4 and 7
    if a & SEP_A_MASK != SEP_A || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    // digits: replace separators with '0' and check all
    let a_d = (a & !SEP_A_MASK) | (0x3000_0030_0000_0000);
    let b_d = (b & !0x0000_FF00_00FF_0000u64) | 0x0000_3000_0030_0000; // 'T' at 2, ':' at 5
    let c_d = (c & !0x0000_00FFu32 & !0x0000_FF00u32) | 0x0000_3030; // keep SS (bytes 2,3)
    let cc = ((c_d as u64) << 32) | 0x3030_3030;
    if !all_digits8(a_d) || !all_digits8(b_d) || !all_digits8(cc) {
        return None;
    }
    let ab = a.to_le_bytes();
    let bb = b.to_le_bytes();
    let dv = |x: u8| (x - b'0') as i64;
    let y = dv(ab[0]) * 1000 + dv(ab[1]) * 100 + dv(ab[2]) * 10 + dv(ab[3]);
    let mo = dv(ab[5]) * 10 + dv(ab[6]);
    let d = dv(bb[0]) * 10 + dv(bb[1]);
    let h = dv(bb[3]) * 10 + dv(bb[4]);
    let mi = dv(bb[6]) * 10 + dv(bb[7]);
    let se = dv(s[17]) * 10 + dv(s[18]);
    if !(1..=12).contains(&mo) || d < 1 || h > 23 || mi > 59 || se > 59 {
        return None;
    }
    let leap = is_leap(y);
    let dim = match mo {
        2 => 28 + leap as i64,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if d > dim {
        return None;
    }
    let yy = y - 1;
    let days_before_year = 365 * (y - 1970) + (yy / 4 - yy / 100 + yy / 400) - 477; // 477 = leaps before 1970
    let days = days_before_year + CUM_DAYS[mo as usize] + (leap && mo > 2) as i64 + d - 1;
    let mut t = ((days * 24 + h) * 60 + mi) * 60 + se;
    if s.len() == 20 {
        if s[19] != b'Z' {
            return None;
        }
    } else {
        let sign = match s[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        if s[22] != b':' {
            return None;
        }
        let oh = dig2(s[20], s[21])?;
        let om = dig2(s[23], s[24])?;
        if oh > 23 || om > 59 {
            return None;
        }
        t -= sign * (oh * 3600 + om * 60);
    }
    Some(t * 1000)
}

// ------------------------------------------------------------------ records

/// Event copy as parsed, before dedup (40 bytes).
#[derive(Clone, Copy)]
#[repr(C)]
struct ERaw {
    key: u64,
    person: u64,
    uref: u64,
    ts: u32,     // ms since window start
    ingest: u32, // seconds, offset from ctx.ing_base
    price: u32,
    cat: u8,
    flags: u8, // 1 = valid (incl. not late), 2 = purchase
}

/// Surviving event (24 bytes).
#[derive(Clone, Copy)]
#[repr(C)]
struct Ev {
    person: u64,
    uref: u64,
    ts: u32,
    price: u32,
    cat: u8,
    purchase: bool,
}

/// Valid click (24 bytes).
#[derive(Clone, Copy)]
#[repr(C)]
struct Click {
    person: u64,
    cref: u64,
    ts: u32, // ms since window start - 7 days
    camp: u32,
}

#[derive(Clone, Copy)]
struct URow {
    person: u64,
    uref: u64,
    day: i32,
    sessions: u32,
    events: u32,
    top: u16, // u16::MAX = null
    revenue: u64,
}

// ------------------------------------------------------------------ context

struct Ctx {
    ws: i64,
    we: i64,
    ws_day: i64,
    ndays: usize,
    ing_base: i64, // seconds
    products: IdMap<u64, u8>,
    unknown_cat: u8,
    cats: Vec<Vec<u8>>,
    campaigns: IdMap<u64, u32>,
    camp_ids: Vec<Vec<u8>>,
    camp_ch: Vec<Vec<u8>>,
    links: LinkTab,
}

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false) && p.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

fn map_file(p: &Path) -> Option<Mmap> {
    let f = File::open(p).expect("open");
    let len = f.metadata().map(|m| m.len()).unwrap_or(0);
    if len == 0 {
        return None;
    }
    let m = if std::env::var("POPULATE").is_ok() {
        unsafe { memmap2::MmapOptions::new().populate().map(&f).expect("mmap") }
    } else {
        unsafe { Mmap::map(&f).expect("mmap") }
    };
    Some(m)
}

fn manifest_value(text: &str, key: &str) -> i64 {
    let k = format!("\"{}\"", key);
    let i = text.find(&k).expect("manifest key") + k.len();
    let rest = &text[i..];
    let a = rest.find('"').unwrap() + 1;
    let b = a + rest[a..].find('"').unwrap();
    parse_ts(rest[a..b].as_bytes()).expect("manifest ts")
}

fn run_threads<F: Fn(usize) + Sync>(n: usize, f: F) {
    std::thread::scope(|s| {
        for t in 0..n {
            let f = &f;
            s.spawn(move || f(t));
        }
    });
}

/// Hand out work items 0..n to threads.
struct Queue(AtomicUsize);
impl Queue {
    fn new() -> Self {
        Queue(AtomicUsize::new(0))
    }
    #[inline]
    fn next(&self) -> usize {
        self.0.fetch_add(1, AO::Relaxed)
    }
}

struct Slots<T>(Vec<std::sync::Mutex<Option<T>>>);
impl<T> Slots<T> {
    fn new(n: usize) -> Self {
        Slots((0..n).map(|_| std::sync::Mutex::new(None)).collect())
    }
    fn put(&self, i: usize, v: T) {
        *self.0[i].lock().unwrap() = Some(v);
    }
    fn take(self) -> Vec<T> {
        self.0.into_iter().map(|m| m.into_inner().unwrap().unwrap()).collect()
    }
}

fn par_map<T: Send, F: Fn(usize) -> T + Sync>(nt: usize, f: F) -> Vec<T> {
    let slots: Slots<T> = Slots::new(nt);
    run_threads(nt, |t| slots.put(t, f(t)));
    slots.take()
}

fn small_csv(file: usize, mut f: impl FnMut(&'static [u8], &'static [u8])) {
    let d = fdata(file);
    let mut ch = Vec::new();
    chunks_of(file, &mut ch);
    for &(_, s, e) in &ch {
        scan::<2>(d, s, e, |_, fl| f(&d[fl[0].0..fl[0].1], &d[fl[1].0..fl[1].1]));
    }
}

fn main() {
    let t0 = Instant::now();
    let data_dir = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out_dir = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let nt: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8))
        .clamp(1, 64);

    // ---- files
    let mut paths: Vec<PathBuf> = vec![data_dir.join("products.csv"), data_dir.join("campaigns.csv")];
    let ev_paths = list_csv(&data_dir.join("events"));
    let cl_paths = list_csv(&data_dir.join("clicks"));
    let ln_paths = list_csv(&data_dir.join("links"));
    let ev_base = paths.len();
    paths.extend(ev_paths.iter().cloned());
    let cl_base = paths.len();
    paths.extend(cl_paths.iter().cloned());
    let ln_base = paths.len();
    paths.extend(ln_paths.iter().cloned());
    let n_files = paths.len();
    assert!(n_files < (1 << (64 - OFF_BITS - LEN_BITS)), "too many files");
    let maps: Vec<Option<Mmap>> = {
        let slots: Slots<Option<Mmap>> = Slots::new(n_files);
        let q = Queue::new();
        run_threads(nt.min(n_files), |_| loop {
            let i = q.next();
            if i >= n_files {
                break;
            }
            slots.put(i, map_file(&paths[i]));
        });
        slots.take()
    };
    let _ = FILES.set(maps);
    let t_map = t0.elapsed();

    let manifest = std::fs::read_to_string(data_dir.join("manifest.json")).expect("manifest");
    let ws = manifest_value(&manifest, "window_start");
    let we = manifest_value(&manifest, "window_end");
    assert!(we - ws + WEEK_MS < u32::MAX as i64, "window too long for u32 offsets");
    let ws_day = ws.div_euclid(DAY_MS);
    let ndays = ((we - 1).div_euclid(DAY_MS) - ws_day + 1).max(1) as usize;

    // ---- products / categories
    let mut prod_raw: Vec<(&'static [u8], &'static [u8])> = Vec::new();
    small_csv(0, |a, b| prod_raw.push((a, b)));
    let mut cats: Vec<Vec<u8>> = prod_raw.iter().map(|p| p.1.to_vec()).collect();
    cats.push(b"unknown".to_vec());
    cats.sort();
    cats.dedup();
    assert!(cats.len() < 256, "too many categories");
    let unknown_cat = cats.binary_search(&b"unknown".to_vec()).unwrap() as u8;
    let mut products: IdMap<u64, u8> = IdMap::with_capacity_and_hasher(prod_raw.len(), Default::default());
    for (p, c) in &prod_raw {
        products.entry(uhash(p)).or_insert(cats.binary_search(&c.to_vec()).unwrap() as u8);
    }

    // ---- campaigns
    let mut campaigns: IdMap<u64, u32> = IdMap::default();
    let mut camp_ids = Vec::new();
    let mut camp_ch = Vec::new();
    small_csv(1, |id, ch| {
        if !campaigns.contains_key(&uhash(id)) {
            campaigns.insert(uhash(id), camp_ids.len() as u32);
            camp_ids.push(id.to_vec());
            camp_ch.push(ch.to_vec());
        }
    });

    // ---- links
    let links = build_links(ln_base, ln_paths.len(), nt);
    let t_links = t0.elapsed();

    let ctx = Arc::new(Ctx {
        ws,
        we,
        ws_day,
        ndays,
        ing_base: ws / 1000 - (1i64 << 31),
        products,
        unknown_cat,
        cats,
        campaigns,
        camp_ids,
        camp_ch,
        links,
    });

    // ---- parse events + clicks
    let mut work: Vec<(usize, usize, usize)> = Vec::new();
    for i in 0..ev_paths.len() {
        chunks_of(ev_base + i, &mut work);
    }
    let n_ev_chunks = work.len();
    for i in 0..cl_paths.len() {
        chunks_of(cl_base + i, &mut work);
    }
    let ev_bytes: usize = (0..ev_paths.len()).map(|i| fdata(ev_base + i).len()).sum();
    let cl_bytes: usize = (0..cl_paths.len()).map(|i| fdata(cl_base + i).len()).sum();
    let ev_line = avg_line(ev_base, ev_paths.len(), 95);
    let cl_line = avg_line(cl_base, cl_paths.len(), 55);
    let est_ev = (ev_bytes as f64 / ev_line * 1.2 / (nt * P1) as f64) as usize + 64;
    let est_cl = (cl_bytes as f64 / cl_line * 1.2 / (nt * P2) as f64) as usize + 64;

    type Parsed = (Vec<Vec<ERaw>>, Vec<Vec<Click>>);
    let parsed: Vec<Parsed> = {
        let q = Queue::new();
        let ctx = &ctx;
        let work = &work;
        par_map(nt, |_| {
            let mut eb: Vec<Vec<ERaw>> = (0..P1).map(|_| Vec::with_capacity(est_ev)).collect();
            let mut cb: Vec<Vec<Click>> = (0..P2).map(|_| Vec::with_capacity(est_cl)).collect();
            loop {
                let i = q.next();
                if i >= work.len() {
                    break;
                }
                let (file, s, e) = work[i];
                if i < n_ev_chunks {
                    parse_events(ctx, file, s, e, &mut eb);
                } else {
                    parse_clicks(ctx, file, s, e, &mut cb);
                }
            }
            (eb, cb)
        })
    };
    let t_parse = t0.elapsed();

    // ---- dedup per event_id partition
    let n_ev: usize = parsed.iter().map(|(eb, _)| eb.iter().map(|b| b.len()).sum::<usize>()).sum();
    let est_out = n_ev * 11 / 10 / (nt * P2) + 64;
    let deduped: Vec<Vec<Vec<Ev>>> = {
        let q = Queue::new();
        let parsed = &parsed;
        par_map(nt, |_| {
            let mut out: Vec<Vec<Ev>> = (0..P2).map(|_| Vec::with_capacity(est_out)).collect();
            let mut tab: Vec<(u64, u32, u32)> = Vec::new();
            loop {
                let p = q.next();
                if p >= P1 {
                    break;
                }
                let total: usize = parsed.iter().map(|(eb, _)| eb[p].len()).sum();
                let cap = (total * 2).next_power_of_two().max(16);
                let mask = cap - 1;
                tab.clear();
                tab.resize(cap, (0, u32::MAX, 0));
                for (ti, (eb, _)) in parsed.iter().enumerate() {
                    for (i, r) in eb[p].iter().enumerate() {
                        let pos = ((ti as u32) << 26) | i as u32;
                        let mut j = (r.key as usize) & mask;
                        loop {
                            let s = unsafe { tab.get_unchecked_mut(j) };
                            if s.1 == u32::MAX {
                                *s = (r.key, pos, r.ingest);
                                break;
                            }
                            if s.0 == r.key {
                                if r.ingest < s.2 {
                                    s.1 = pos;
                                    s.2 = r.ingest;
                                }
                                break;
                            }
                            j = (j + 1) & mask;
                        }
                    }
                }
                for s in tab.iter() {
                    if s.1 == u32::MAX {
                        continue;
                    }
                    let r = unsafe { parsed.get_unchecked((s.1 >> 26) as usize).0.get_unchecked(p).get_unchecked((s.1 & ((1 << 26) - 1)) as usize) };
                    if r.flags & 1 != 0 {
                        out[(r.person >> (64 - P2_BITS)) as usize].push(Ev {
                            person: r.person,
                            uref: r.uref,
                            ts: r.ts,
                            price: r.price,
                            cat: r.cat,
                            purchase: r.flags & 2 != 0,
                        });
                    }
                }
            }
            out
        })
    };
    let t_dedup = t0.elapsed();

    // ---- sessionize + attribute per person partition
    let nc = ctx.camp_ids.len();
    let ncamp = nc + 1; // + organic
    type Agg = (Vec<URow>, Vec<[u64; 4]>);
    let aggs: Vec<Agg> = {
        let q = Queue::new();
        let ctx = &ctx;
        let deduped = &deduped;
        let parsed = &parsed;
        par_map(nt, |_| {
            let mut rows: Vec<URow> = Vec::with_capacity(n_ev / 8 / nt + 64);
            let mut camp = vec![[0u64; 4]; ncamp * ctx.ndays];
            let mut st = PersonState::new(ctx.cats.len(), nc);
            let mut evs: Vec<Ev> = Vec::new();
            let mut cls: Vec<Click> = Vec::new();
            let mut counts: Vec<u32> = Vec::new();
            loop {
                let p = q.next();
                if p >= P2 {
                    break;
                }
                let esrc: Vec<&[Ev]> = deduped.iter().map(|d| &d[p][..]).collect();
                bucket_sort(&esrc, &mut evs, &mut counts, |e| e.person, |a, b| (a.person, a.ts) < (b.person, b.ts));
                if evs.is_empty() {
                    continue;
                }
                let csrc: Vec<&[Click]> = parsed.iter().map(|(_, cb)| &cb[p][..]).collect();
                bucket_sort(&csrc, &mut cls, &mut counts, |c| c.person, |a, b| {
                    (a.person, a.ts).cmp(&(b.person, b.ts)).then_with(|| rbytes(a.cref).cmp(rbytes(b.cref))) == std::cmp::Ordering::Less
                });
                let mut ci = 0usize;
                let mut i = 0usize;
                while i < evs.len() {
                    let person = evs[i].person;
                    let mut j = i + 1;
                    while j < evs.len() && evs[j].person == person {
                        j += 1;
                    }
                    while ci < cls.len() && cls[ci].person < person {
                        ci += 1;
                    }
                    let mut cj = ci;
                    while cj < cls.len() && cls[cj].person == person {
                        cj += 1;
                    }
                    st.person(ctx, &evs[i..j], &cls[ci..cj], &mut rows, &mut camp);
                    ci = cj;
                    i = j;
                }
            }
            (rows, camp)
        })
    };
    let t_sess = t0.elapsed();

    // ---- merge campaign aggregates
    let mut camp = vec![[0u64; 4]; ncamp * ndays];
    let mut user_rows: Vec<Vec<URow>> = Vec::new();
    for (rows, c) in aggs {
        for (a, b) in camp.iter_mut().zip(c.iter()) {
            for k in 0..4 {
                a[k] += b[k];
            }
        }
        user_rows.push(rows);
    }

    write_outputs(&ctx, &out_dir, user_rows, &camp, nt);
    let t_end = t0.elapsed();
    eprintln!(
        "map {:.3}s links {:.3}s parse {:.3}s dedup {:.3}s sess {:.3}s write {:.3}s",
        t_map.as_secs_f64(),
        (t_links - t_map).as_secs_f64(),
        (t_parse - t_links).as_secs_f64(),
        (t_dedup - t_parse).as_secs_f64(),
        (t_sess - t_dedup).as_secs_f64(),
        (t_end - t_sess).as_secs_f64()
    );
    std::mem::forget(parsed);
    std::mem::forget(deduped);
    // skip destructors of large structures
    std::process::exit(0);
}

/// Gather `src` into `out` sorted by `less`, using a counting pass on the
/// person-hash bits just below the partition bits, then small sorts.
fn bucket_sort<T: Copy>(src: &[&[T]], out: &mut Vec<T>, counts: &mut Vec<u32>, key: impl Fn(&T) -> u64, less: impl Fn(&T, &T) -> bool + Copy) {
    let total: usize = src.iter().map(|s| s.len()).sum();
    out.clear();
    if total == 0 {
        return;
    }
    let bb = ((total / 4).max(2).ilog2()).clamp(1, 18);
    let shift = 64 - P2_BITS - bb;
    let mask = (1u64 << bb) - 1;
    let nb = 1usize << bb;
    counts.clear();
    counts.resize(nb + 1, 0);
    for s in src {
        for x in s.iter() {
            counts[((key(x) >> shift) & mask) as usize + 1] += 1;
        }
    }
    for i in 0..nb {
        counts[i + 1] += counts[i];
    }
    out.reserve(total);
    unsafe {
        out.set_len(total);
    }
    {
        let o = out.as_mut_ptr();
        for s in src {
            for x in s.iter() {
                let b = ((key(x) >> shift) & mask) as usize;
                let c = unsafe { counts.get_unchecked_mut(b) };
                unsafe { o.add(*c as usize).write(*x) };
                *c += 1;
            }
        }
    }
    // counts[b] is now the end of bucket b; bucket b starts at counts[b-1] (0 for b = 0)
    let mut start = 0usize;
    for b in 0..nb {
        let end = counts[b] as usize;
        let sl = &mut out[start..end];
        if sl.len() > 1 {
            if sl.len() <= 20 {
                for i in 1..sl.len() {
                    let x = sl[i];
                    let mut j = i;
                    while j > 0 && less(&x, &sl[j - 1]) {
                        sl[j] = sl[j - 1];
                        j -= 1;
                    }
                    sl[j] = x;
                }
            } else {
                sl.sort_unstable_by(|a, b| {
                    if less(a, b) {
                        std::cmp::Ordering::Less
                    } else if less(b, a) {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Equal
                    }
                });
            }
        }
        start = end;
    }
}

/// Average line length over a sample of the largest file in a group.
fn avg_line(base: usize, n: usize, default: usize) -> f64 {
    let mut best = None;
    for i in 0..n {
        let d = fdata(base + i);
        if best.map(|(l, _)| d.len() > l).unwrap_or(true) {
            best = Some((d.len(), base + i));
        }
    }
    if let Some((len, f)) = best {
        let d = fdata(f);
        let sample = &d[..len.min(1 << 20)];
        let lines = memchr::memchr_iter(b'\n', sample).count();
        if lines > 10 {
            return sample.len() as f64 / lines as f64;
        }
    }
    default as f64
}

// ------------------------------------------------------------------ links

fn build_links(base: usize, n: usize, nt: usize) -> LinkTab {
    use std::sync::atomic::{AtomicU32, AtomicU64};
    let mut work = Vec::new();
    for i in 0..n {
        chunks_of(base + i, &mut work);
    }
    let bytes: usize = (0..n).map(|i| fdata(base + i).len()).sum();
    let est_lines = (bytes as f64 / avg_line(base, n, 22) * 1.1) as usize + 16;
    let cap = (est_lines * 3).next_power_of_two().max(1024);
    let mask = cap - 1;
    fn z64(n: usize) -> Vec<AtomicU64> {
        let mut v = std::mem::ManuallyDrop::new(vec![0u64; n]);
        unsafe { Vec::from_raw_parts(v.as_mut_ptr() as *mut AtomicU64, v.len(), v.capacity()) }
    }
    fn z32(n: usize) -> Vec<AtomicU32> {
        let mut v = std::mem::ManuallyDrop::new(vec![0u32; n]);
        unsafe { Vec::from_raw_parts(v.as_mut_ptr() as *mut AtomicU32, v.len(), v.capacity()) }
    }
    let keys = z64(cap);
    let refs = z64(cap);
    // parent stores parent + 1; 0 means "is a root"
    let parent = z32(cap);
    let insert = |h: u64, r: u64| -> u32 {
        let mut i = (h as usize) & mask;
        loop {
            let k = keys[i].load(AO::Relaxed);
            if k == h {
                return i as u32;
            }
            if k == 0 {
                match keys[i].compare_exchange(0, h, AO::AcqRel, AO::Acquire) {
                    Ok(_) => {
                        refs[i].store(r, AO::Relaxed);
                        return i as u32;
                    }
                    Err(cur) => {
                        if cur == h {
                            return i as u32;
                        }
                    }
                }
            }
            i = (i + 1) & mask;
        }
    };
    let find = |mut x: u32| -> u32 {
        loop {
            let p1 = parent[x as usize].load(AO::Relaxed);
            if p1 == 0 {
                return x;
            }
            let p = p1 - 1;
            let gp1 = parent[p as usize].load(AO::Relaxed);
            if gp1 == 0 {
                return p;
            }
            let _ = parent[x as usize].compare_exchange(p1, gp1, AO::Relaxed, AO::Relaxed);
            x = gp1 - 1;
        }
    };
    let union = |a: u32, b: u32| {
        let (mut a, mut b) = (a, b);
        loop {
            a = find(a);
            b = find(b);
            if a == b {
                return;
            }
            if a < b {
                std::mem::swap(&mut a, &mut b);
            }
            // attach the larger root under the smaller one
            if parent[a as usize].compare_exchange(0, b + 1, AO::AcqRel, AO::Relaxed).is_ok() {
                return;
            }
        }
    };
    {
        let q = Queue::new();
        let work = &work;
        run_threads(nt, |_| loop {
            let i = q.next();
            if i >= work.len() {
                break;
            }
            let (file, s, e) = work[i];
            let d = fdata(file);
            scan::<2>(d, s, e, |_, f| {
                let (a0, a1) = f[0];
                let (b0, b1) = f[1];
                if a0 == a1 || b0 == b1 {
                    return;
                }
                let sa = insert(uhash(&d[a0..a1]), mkref(file, a0, a1 - a0));
                let sb = insert(uhash(&d[b0..b1]), mkref(file, b0, b1 - b0));
                if sa != sb {
                    union(sa, sb);
                }
            });
        });
    }
    // name of each root: byte-wise smallest ID (refs written before the key became visible
    // to other threads may still be 0 only transiently; all inserts are done now)
    let best = z32(cap); // best slot + 1; 0 = none yet
    let per = cap.div_ceil(nt);
    run_threads(nt, |t| {
        for i in t * per..((t + 1) * per).min(cap) {
            if keys[i].load(AO::Relaxed) == 0 {
                continue;
            }
            let r = find(i as u32) as usize;
            let mine = rbytes(refs[i].load(AO::Relaxed));
            let mut cur = best[r].load(AO::Relaxed);
            loop {
                if cur != 0 && rbytes(refs[cur as usize - 1].load(AO::Relaxed)) <= mine {
                    break;
                }
                match best[r].compare_exchange(cur, i as u32 + 1, AO::AcqRel, AO::Relaxed) {
                    Ok(_) => break,
                    Err(c) => cur = c,
                }
            }
        }
    });
    let mut slots: Vec<[u64; 2]> = Vec::with_capacity(cap);
    let mut nrefs: Vec<u64> = Vec::with_capacity(cap);
    unsafe {
        slots.set_len(cap);
        nrefs.set_len(cap);
    }
    {
        let sp = slots.as_mut_ptr() as usize;
        let np = nrefs.as_mut_ptr() as usize;
        run_threads(nt, |t| {
            for i in t * per..((t + 1) * per).min(cap) {
                let k = keys[i].load(AO::Relaxed);
                let (pk, nr) = if k == 0 {
                    (0, 0)
                } else {
                    let r = find(i as u32) as usize;
                    let b = best[r].load(AO::Relaxed) as usize - 1;
                    (keys[b].load(AO::Relaxed), refs[i].load(AO::Relaxed))
                };
                unsafe {
                    (sp as *mut [u64; 2]).add(i).write([k, pk]);
                    (np as *mut u64).add(i).write(nr);
                }
            }
        });
    }
    LinkTab { mask, slots, refs: nrefs }
}

// ------------------------------------------------------------------ parsing

#[inline]
fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() || s.len() > 20 {
        return None;
    }
    let mut v: u64 = 0;
    let mut sig = 0;
    for &c in s {
        let x = c.wrapping_sub(b'0');
        if x >= 10 {
            return None;
        }
        if sig > 0 || x != 0 {
            sig += 1;
            if sig > 8 {
                return None;
            }
            v = v * 10 + x as u64;
        }
    }
    if v >= 1 && v <= 10_000_000 { Some(v as u32) } else { None }
}

const RING: usize = 16;

fn parse_events(ctx: &Ctx, file: usize, s: usize, e: usize, eb: &mut [Vec<ERaw>]) {
    let d = fdata(file);
    let mut ring = [ERaw { key: 0, person: 0, uref: 0, ts: 0, ingest: 0, price: 0, cat: 0, flags: 0 }; RING];
    let mut rpart = [0usize; RING];
    let mut rn = 0usize; // number of records pushed into the ring so far
    let links = &ctx.links;
    scan::<7>(d, s, e, |_, f| {
        let g = |k: usize| &d[f[k].0..f[k].1];
        let eid = g(0);
        if eid.is_empty() {
            return;
        }
        let h = xxh3_128(eid);
        let part = (h >> (128 - P1_BITS)) as usize;
        let ingest_ms = match parse_ts(g(3)) {
            Some(t) => t,
            None => i64::MAX,
        };
        let ingest = if ingest_ms == i64::MAX {
            u32::MAX
        } else {
            (ingest_ms / 1000 - ctx.ing_base).clamp(0, u32::MAX as i64 - 1) as u32
        };
        let mut r = ERaw { key: h as u64, person: 0, uref: 0, ts: 0, ingest, price: 0, cat: 0, flags: 0 };
        'v: {
            let user = g(1);
            if user.is_empty() {
                break 'v;
            }
            let ts = match parse_ts(g(2)) {
                Some(t) if t >= ctx.ws && t < ctx.we => t,
                _ => break 'v,
            };
            let purchase = match g(4) {
                b"view" | b"cart" => false,
                b"purchase" => true,
                _ => break 'v,
            };
            let prod = g(5);
            if prod.is_empty() {
                break 'v;
            }
            let mut price = 0;
            if purchase {
                price = match parse_price(g(6)) {
                    Some(p) => p,
                    None => break 'v,
                };
            }
            if ingest_ms == i64::MAX || ingest_ms - ts > DAY_MS {
                break 'v;
            }
            let uh = uhash(user);
            links.pf(uh);
            r.person = uh;
            r.uref = mkref(file, f[1].0, user.len());
            r.ts = (ts - ctx.ws) as u32;
            r.price = price;
            r.cat = *ctx.products.get(&uhash(prod)).unwrap_or(&ctx.unknown_cat);
            r.flags = 1 | if purchase { 2 } else { 0 };
            // pipeline: resolve the record pushed RING lines ago
            let k = rn % RING;
            if rn >= RING {
                let mut o = ring[k];
                o.person = links.person(o.person);
                eb[rpart[k]].push(o);
            }
            ring[k] = r;
            rpart[k] = part;
            rn += 1;
            return;
        }
        eb[part].push(r);
    });
    let start = rn.saturating_sub(RING);
    for i in start..rn {
        let k = i % RING;
        let mut o = ring[k];
        o.person = links.person(o.person);
        eb[rpart[k]].push(o);
    }
}

fn parse_clicks(ctx: &Ctx, file: usize, s: usize, e: usize, cb: &mut [Vec<Click>]) {
    let d = fdata(file);
    let lo = ctx.ws - WEEK_MS;
    let links = &ctx.links;
    let mut ring = [Click { person: 0, cref: 0, ts: 0, camp: 0 }; RING];
    let mut rn = 0usize;
    let emit = |mut o: Click, cb: &mut [Vec<Click>]| {
        o.person = links.person(o.person);
        cb[(o.person >> (64 - P2_BITS)) as usize].push(o);
    };
    scan::<4>(d, s, e, |_, f| {
        let g = |k: usize| &d[f[k].0..f[k].1];
        let cid = g(0);
        if cid.is_empty() {
            return;
        }
        let user = g(1);
        if user.is_empty() {
            return;
        }
        let ts = match parse_ts(g(3)) {
            Some(t) if t >= lo && t < ctx.we => t,
            _ => return,
        };
        let camp = match ctx.campaigns.get(&uhash(g(2))) {
            Some(&c) => c,
            None => return,
        };
        let uh = uhash(user);
        links.pf(uh);
        let c = Click { person: uh, cref: mkref(file, f[0].0, cid.len()), ts: (ts - lo) as u32, camp };
        let k = rn % RING;
        if rn >= RING {
            emit(ring[k], cb);
        }
        ring[k] = c;
        rn += 1;
    });
    let start = rn.saturating_sub(RING);
    for i in start..rn {
        emit(ring[i % RING], cb);
    }
}

// ------------------------------------------------------------------ per person

struct Sess {
    start: u32, // ms since window start
    day: u32,   // index from window start day
    revenue: u64,
    purchase: bool,
}

struct PersonState {
    catrev: Vec<u64>,
    sess: Vec<Sess>,
    run_end: Vec<u32>,
    cnt: Vec<u32>,
    act: Vec<u32>,
}

impl PersonState {
    fn new(ncat: usize, nc: usize) -> Self {
        PersonState { catrev: vec![0; ncat], sess: Vec::new(), run_end: Vec::new(), cnt: vec![0; nc], act: vec![0; nc] }
    }

    fn person(&mut self, ctx: &Ctx, evs: &[Ev], cls: &[Click], rows: &mut Vec<URow>, camp: &mut [[u64; 4]]) {
        let nd = ctx.ndays;
        let person = evs[0].person;
        let uref = evs[0].uref;
        let off_day = ctx.ws.div_euclid(DAY_MS) * DAY_MS - ctx.ws; // <= 0, usually 0
        self.sess.clear();
        let mut cur_day: u32 = u32::MAX;
        let mut row = URow { person, uref, day: 0, sessions: 0, events: 0, top: u16::MAX, revenue: 0 };
        let mut any = false;
        let mut prev: u32 = 0;
        let mut first = true;
        for e in evs {
            if first || e.ts - prev > GAP_MS {
                first = false;
                let day = ((e.ts as i64 - off_day) / DAY_MS) as u32;
                if day != cur_day {
                    if cur_day != u32::MAX {
                        self.flush_row(&mut row, any, rows);
                    }
                    cur_day = day;
                    row = URow { person, uref, day: (ctx.ws_day + day as i64) as i32, sessions: 0, events: 0, top: u16::MAX, revenue: 0 };
                    any = false;
                }
                row.sessions += 1;
                self.sess.push(Sess { start: e.ts, day, revenue: 0, purchase: false });
            }
            prev = e.ts;
            row.events += 1;
            if e.purchase {
                let p = e.price as u64;
                row.revenue += p;
                self.catrev[e.cat as usize] += p;
                any = true;
                let s = self.sess.last_mut().unwrap();
                s.revenue += p;
                s.purchase = true;
            }
        }
        self.flush_row(&mut row, any, rows);

        let n = self.sess.len();
        let organic = ctx.camp_ids.len();
        if cls.is_empty() {
            for s in &self.sess {
                let c = &mut camp[organic * nd + s.day as usize];
                c[0] += 1;
                c[1] += s.purchase as u64;
                c[2] += s.revenue;
            }
            return;
        }
        self.run_end.clear();
        self.run_end.resize(n, 0);
        let mut k = n;
        while k > 0 {
            let i = k - 1;
            self.run_end[i] = if i + 1 < n && self.sess[i + 1].day == self.sess[i].day { self.run_end[i + 1] } else { (i + 1) as u32 };
            k -= 1;
        }
        // click ts are offsets from ws - 7d; session start in the same frame
        let mut lo = 0usize;
        let mut hi = 0usize;
        for i in 0..n {
            let s = self.sess[i].start as i64 + WEEK_MS; // click frame
            while hi < cls.len() && (cls[hi].ts as i64) <= s {
                let c = cls[hi].camp as usize;
                if self.cnt[c] == 0 {
                    self.act[c] = i as u32;
                }
                self.cnt[c] += 1;
                hi += 1;
            }
            while lo < hi && (cls[lo].ts as i64) < s - WEEK_MS {
                let c = cls[lo].camp as usize;
                self.cnt[c] -= 1;
                if self.cnt[c] == 0 {
                    self.emit(nd, c, self.act[c] as usize, i, camp);
                }
                lo += 1;
            }
            let credited = if hi > lo {
                let mut k = hi - 1;
                while k > lo && cls[k - 1].ts == cls[k].ts {
                    k -= 1;
                }
                cls[k].camp as usize
            } else {
                organic
            };
            let ss = &self.sess[i];
            let c = &mut camp[credited * nd + ss.day as usize];
            c[0] += 1;
            c[1] += ss.purchase as u64;
            c[2] += ss.revenue;
        }
        for k in lo..hi {
            let c = cls[k].camp as usize;
            if self.cnt[c] > 0 {
                self.cnt[c] = 0;
                self.emit(nd, c, self.act[c] as usize, n, camp);
            }
        }
    }

    #[inline]
    fn emit(&self, nd: usize, c: usize, a: usize, b: usize, camp: &mut [[u64; 4]]) {
        let mut i = a;
        while i < b {
            let e = (self.run_end[i] as usize).min(b);
            camp[c * nd + self.sess[i].day as usize][3] += (e - i) as u64;
            i = e;
        }
    }

    #[inline]
    fn flush_row(&mut self, row: &mut URow, any: bool, rows: &mut Vec<URow>) {
        if any {
            let mut best = 0usize;
            for k in 1..self.catrev.len() {
                if self.catrev[k] > self.catrev[best] {
                    best = k;
                }
            }
            row.top = best as u16;
            self.catrev.iter_mut().for_each(|x| *x = 0);
        }
        rows.push(*row);
    }
}

// ------------------------------------------------------------------ output

fn write_outputs(ctx: &Ctx, out: &Path, user_rows: Vec<Vec<URow>>, camp: &[[u64; 4]], nt: usize) {
    use arrow_array::{ArrayRef, Date32Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};

    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud).unwrap();
    std::fs::create_dir_all(&cd).unwrap();

    let props = || {
        WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_max_row_group_row_count(Some(1 << 20))
            .build()
    };

    let user_schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]));

    let cats: Vec<String> = ctx.cats.iter().map(|c| String::from_utf8_lossy(c).into_owned()).collect();
    let q = Queue::new();
    let ur = &user_rows;
    let cats = &cats;
    let user_schema = &user_schema;
    run_threads(nt.min(user_rows.len()).max(1), |_| loop {
        let i = q.next();
        if i >= ur.len() {
            break;
        }
        let rows = &ur[i];
        let name = |r: &URow| -> &'static [u8] {
            match ctx.links.name(r.person) {
                Some(nr) => rbytes(nr),
                None => rbytes(r.uref),
            }
        };
        let ids: StringArray = rows.iter().map(|r| Some(String::from_utf8_lossy(name(r)))).collect();
        let days = Date32Array::from_iter_values(rows.iter().map(|r| r.day));
        let s = Int64Array::from_iter_values(rows.iter().map(|r| r.sessions as i64));
        let ev = Int64Array::from_iter_values(rows.iter().map(|r| r.events as i64));
        let rev = Int64Array::from_iter_values(rows.iter().map(|r| r.revenue as i64));
        let top: StringArray = rows.iter().map(|r| if r.top == u16::MAX { None } else { Some(cats[r.top as usize].as_str()) }).collect();
        let batch = RecordBatch::try_new(
            user_schema.clone(),
            vec![Arc::new(ids) as ArrayRef, Arc::new(days), Arc::new(s), Arc::new(ev), Arc::new(rev), Arc::new(top)],
        )
        .unwrap();
        let f = File::create(ud.join(format!("part-{:05}.parquet", i))).unwrap();
        let mut w = ArrowWriter::try_new(std::io::BufWriter::with_capacity(1 << 20, f), user_schema.clone(), Some(props())).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    });

    // campaign_daily
    let nd = ctx.ndays;
    let nc = ctx.camp_ids.len();
    let mut cid = Vec::new();
    let mut ch = Vec::new();
    let mut day = Vec::new();
    let mut cols = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
    for c in 0..=nc {
        for d in 0..nd {
            let a = camp[c * nd + d];
            if a[0] == 0 && a[3] == 0 {
                continue;
            }
            if c == nc {
                cid.push("organic".to_string());
                ch.push("organic".to_string());
            } else {
                cid.push(String::from_utf8_lossy(&ctx.camp_ids[c]).into_owned());
                ch.push(String::from_utf8_lossy(&ctx.camp_ch[c]).into_owned());
            }
            day.push((ctx.ws_day + d as i64) as i32);
            let assisted = if c == nc { 0 } else { a[3] };
            cols[0].push(a[0] as i64);
            cols[1].push(a[1] as i64);
            cols[2].push(a[2] as i64);
            cols[3].push(assisted as i64);
        }
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]));
    let [c0, c1, c2, c3] = cols;
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(cid)) as ArrayRef,
            Arc::new(StringArray::from(ch)),
            Arc::new(Date32Array::from(day)),
            Arc::new(Int64Array::from(c0)),
            Arc::new(Int64Array::from(c1)),
            Arc::new(Int64Array::from(c2)),
            Arc::new(Int64Array::from(c3)),
        ],
    )
    .unwrap();
    let f = File::create(cd.join("part-00000.parquet")).unwrap();
    let mut w = ArrowWriter::try_new(f, schema, Some(props())).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}
