use futures::join;
use futures::stream::StreamExt;
use std::collections::{HashMap, BTreeMap};
use std::fs::File;
use std::io::{
    self,
    BufRead,
    BufReader
};

use tokio::sync::mpsc::{
    channel,
    error::SendError,
    Receiver,
    Sender};
use tokio_stream::wrappers::ReceiverStream;

const RADIX: u32 = 10;

#[tokio::main]
async fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: ./rubric <filepath>");
        std::process::exit(1);
    }
    let filepath = &args[1];
    let file = File::open(filepath)?;
    let reader = BufReader::with_capacity((256 * 1024) as usize, file);

    let (tx, rx) = channel::<Vec<u8>>(128);
    let read_fut = read_to(reader, tx);
    let (stx, srx) = channel::<String>(128);
    let valid_chunks_fut = into_valid_chunks(rx, stx);
    let (htx, hrx) = channel::<HashMap<String, TempStat>>(128);
    let stats_fut = into_stats(srx, htx);
    let aggregate_fut = aggregate_stats(hrx);

    let (_, _, _, aggr_stats) = join!(read_fut, valid_chunks_fut, stats_fut, aggregate_fut);
    for (city_name, stat) in aggr_stats {
        println!(
            "City: {:<20} | Average: {:.2} | Min: {:.2} | Max: {:.2}",
            city_name,
            stat.average(),
            stat.min,
            stat.max
        );
    }

    Ok(())
}

async fn into_valid_chunks(mut rcv: Receiver<Vec<u8>>, snd: Sender<String>) -> io::Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(512 * 1024);
    while let Some(v) = rcv.recv().await {
        let mut it = v.iter();
        let lastidx = it.rposition(|&b| b == b'\n').unwrap();

        let (valid, rest) = v.split_at(lastidx + 1);
        if valid.len() > 0 {
            buf.extend_from_slice(valid);
        }
        let clean_str = String::from_utf8(buf.clone()).unwrap();
        snd.send(clean_str).await.unwrap();
        buf.clear();
        buf.extend_from_slice(rest);
    }

    Ok(())
}

async fn read_to(mut reader: BufReader<File>, tx: Sender<Vec<u8>>) -> io::Result<()> {
    loop {
        let bits = reader.fill_buf()?;
        if bits.is_empty() {
            break;
        }

        let lenb = bits.len();
        let mut vb: Vec<u8> = Vec::with_capacity(lenb);
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

async fn into_stats(rs: Receiver<String>, shm: Sender<HashMap<String, TempStat>>) -> io::Result<()> {
    let mut inner = ReceiverStream::new(rs);
    let mut stream = inner.by_ref()
        .map(|str| parse(str))
        .buffer_unordered(64);

    while let Some(hm) = stream.next().await {
        let _ = shm.send(hm).await;
    }

    Ok(())
}

async fn aggregate_stats(mut rx: Receiver<HashMap<String, TempStat>>) -> BTreeMap<String, TempStat> {
    let mut aggregate: BTreeMap<String, TempStat> = BTreeMap::new();
    while let Some(hm) = rx.recv().await {
        for (city, stat) in hm {
            aggregate.entry(city)
                .and_modify(|aggr| aggr.merge(stat.clone()))
                .or_insert(stat);
        }
    }
    aggregate
}

async fn parse(line: String) -> HashMap<String, TempStat> {
    let mut bytz = line.chars().peekable();
    let mut cities: HashMap<String, TempStat> = HashMap::new();
    let mut is_neg = false;

    while let Some(_) = bytz.peek() {
        let mut city: String = String::with_capacity(40);
        while let Some(b) = bytz.next() {
            match b {
                ';' => break,
                _ => {
                    city.push(b); 
                }
            }
        }
    
        let mut measure: i64 = 0;
        while let Some(b) = bytz.next() {
            match b {
                '\n' => {
                    if is_neg {
                        measure = -measure;
                    }
                    break
                },
                '-' => {
                    is_neg = true;
                },
                '.' => continue,
                _ => {
                    let d = char::to_digit(b, RADIX).unwrap() as i64;
                    measure = measure * 10 + d;
                }
            }
        }
        let _city_stat = cities
            .entry(city)
            .and_modify(|cs| cs.record(measure))
            .or_insert_with(|| TempStat::new(measure));
        is_neg = false;
    }
    cities
}
