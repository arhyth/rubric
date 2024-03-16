mod pool;

use std::{
    hash::BuildHasher,
    collections::{
        BTreeMap,
        HashMap,
    }
};
use std::fs::File;
use std::io::{
    self,
    BufRead,
    BufReader
};
use std::sync::Arc;
use std::sync::mpsc::{
    sync_channel,
    Receiver,
    Sender};
use tokio_stream::wrappers::ReceiverStream;

use once_cell::sync::Lazy;
use once_map::OnceMap;
use tokio::sync::{
    mpsc,
    Semaphore,
};

use crate::pool::{
    Managed,
    Pool,
};

const BYTES_POOL_SIZE: usize = 64;
static STRING_MAP: Lazy<OnceMap<u64, String>> = Lazy::new(|| OnceMap::new());
static BYTES_POOL: Lazy<Pool<Vec<u8>>> = Lazy::new(|| Pool::new(BYTES_POOL_SIZE));

#[tokio::main(max_threads=10)]
async fn main() -> io::Result<()> {
    let file = File::open("/Users/arhyth/git-shit/1brc/measurements.txt")?;
    let reader = BufReader::with_capacity((256 * 1024) as usize, file);

    let (tx, rx) = sync_channel::<Managed<Vec<u8>>>(BYTES_POOL_SIZE/2);
    let (stx, srx) = sync_channel::<Managed<Vec<u8>>>(BYTES_POOL_SIZE/2);
    let (htx, hrx) = mpsc::channel::<HashMap<u64, TempStat>>(BYTES_POOL_SIZE/2);

    let read_handle = tokio::task::spawn_blocking(move || read_to(reader, tx));
    let chunk_handle = tokio::task::spawn_blocking(move || into_valid_chunks(rx, stx));
    let stats_handle = tokio::task::spawn(into_stats(srx, htx));
    let aggr_handle = tokio::task::spawn(aggregate_stats(hrx));

    let (_, _, _, aggr_stats) = tokio::join!(read_handle, chunk_handle, stats_handle, aggr_handle);

    print!("{{");
    for (city_name, stat) in aggr_stats.unwrap() {
        print!(
            "{}=Average: {:.2}/{:.2}/{:.2},",
            STRING_MAP.get(&city_name).expect("SHOULD NOT HAPPEN"),
            stat.min as f32 / 10.0,
            stat.average(),
            stat.max as f32 / 10.0
        );
    }
    print!("}}");

    Ok(())
}

fn read_to(mut reader: BufReader<File>, tx: SyncSender<Managed<Vec<u8>>>) -> () {
    loop {
        let bits = reader.fill_buf()?;
        if bits.is_empty() {
            break;
        }

        let lenb = bits.len();
        let mut vb: Managed<Vec<u8>> = BYTES_POOL.acquire(|| Vec::with_capacity(512 * 1024));
        vb.clear();
        vb.extend_from_slice(bits);
        reader.consume(lenb);

        match tx.send(vb).await {
            Ok(()) => (),
            Err(SendError(_)) => {
                println!("send read FAIL");
            }
        }
    }

    Ok(())
}

fn into_valid_chunks(rcv: Receiver<Managed<Vec<u8>>>, snd: SyncSender<Managed<Vec<u8>>>) -> () {
    let mut buf: Managed<Vec<u8>> = BYTES_POOL.acquire(|| Vec::with_capacity(512 * 1024));
    buf.clear();
    while let Ok(mut v) = rcv.recv() {
        let len = v.len();
        let mut it = v.iter();
        let lastidx = it.rposition(|&b| b == b'\n').unwrap();

        buf.extend_from_slice(&v[..lastidx+1]);
        snd.send(buf).unwrap();
        if lastidx < len - 1 {
            v.copy_within(lastidx+1.., 0);
            v.resize(len-1 - lastidx, 0);
        } else {
            v.clear();
        }
        buf = v;
    }
}

#[derive(Clone)]
struct TempStat {
    sum: i64,
    count: i64,
    min: i64,
    max: i64,
}

impl TempStat {
    fn new(m: i64) -> Self {
        TempStat {
            sum: m,
            count: 1,
            min: m,
            max: m,
        }
    }

    fn record(&mut self, temp: i64) {
        self.sum += temp;
        self.count += 1;
        self.min = self.min.min(temp);
        self.max = self.max.max(temp);
    }

    fn merge(&mut self, stat: TempStat) {
        self.sum += stat.sum;
        self.count += stat.count;
        self.min = self.min.min(stat.min);
        self.max = self.max.max(stat.max);
    }

    fn average(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum as f64 / (self.count * 10) as f64
        }
    }
}

async fn into_stats(rs: Receiver<Managed<Vec<u8>>>, shm: mpsc::Sender<HashMap<u64, TempStat>>) -> () {
    let sem = Arc::new(Semaphore::new(4));
    while let Ok(bits) = rs.recv() {
        let permit = sem.clone().acquire_owned().await;
        let mut shc = shm.clone();
        tokio::spawn(async move {
            let partial_stats = parse(bits);
            let _ = shc.send(partial_stats).await;
            drop(permit);
        });
    }

    Ok(())
}

async fn aggregate_stats(mut rx: mpsc::Receiver<HashMap<u64, TempStat>>) -> BTreeMap<u64, TempStat> {
    let mut aggregate: BTreeMap<u64, TempStat> = BTreeMap::new();
    while let Some(hm) = rx.recv().await {
        for (city, stat) in hm {
            aggregate.entry(city)
                .and_modify(|aggr| aggr.merge(stat.clone()))
                .or_insert(stat);
        }
    }
    aggregate
}

#[allow(unused_assignments)]
fn parse(line: Managed<Vec<u8>>) -> HashMap<u64, TempStat> {
    let mut bytz = line.iter().enumerate().peekable();
    let mut cities: HashMap<u64, TempStat> = HashMap::new();
    let mut is_neg = false;
    let mut city_start: usize = 0;
    let mut city_end: usize = 0;

    while let Some((cstr_start, _)) = bytz.peek() {
        city_start = *cstr_start;
        while let Some((bi, b)) = bytz.next() {
            match *b {
                b';' => {
                    city_end = bi;
                    break
                },
                _ => continue,
            }
        }
        let city = &line[city_start..city_end];
        let hash = STRING_MAP.hasher().hash_one(city);
        if !STRING_MAP.contains_key(&hash) {
            STRING_MAP.insert(
                hash,
                |_| std::str::from_utf8(city)
                    .expect("invalid utf-8 slice")
                    .to_string()
            );
        }
        let mut measure: i64 = 0;
        while let Some((_, b)) = bytz.next() {
            match *b {
                b'\n' => {
                    if is_neg {
                        measure = -measure;
                    }
                    break
                },
                b'-' => {
                    is_neg = true;
                },
                b'.' => continue,
                _ => {
                    let d = (b - b'0') as i64;
                    measure = measure * 10 + d;
                }
            }
        }
        let _city_stat = cities
            .entry(hash)
            .and_modify(|cs| cs.record(measure))
            .or_insert_with(|| TempStat::new(measure));
        is_neg = false;
    }

    // recycle to pool
    line.recycle();

    cities
}
