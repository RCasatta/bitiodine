#[macro_use]
extern crate arrayref;
extern crate base58;
extern crate bitcoin_bech32;
extern crate byteorder;
extern crate crypto;
extern crate memmap;
extern crate rustc_serialize;
extern crate time;
extern crate vec_map;
extern crate void;

extern crate env_logger;
#[macro_use]
extern crate log;

#[macro_use]
mod buffer_operations;

mod address;
mod block;
mod blockchain;
mod bytecode;
mod error;
mod hash;
mod hash160;
mod header;
mod merkle;
mod preamble;
mod script;
mod transactions;
pub mod visitors;

use blockchain::BlockChain;
use env_logger::Builder;
use log::LevelFilter;
use visitors::clusterizer::Clusterizer;
use visitors::BlockChainVisitor;
use visitors::dump_balances::DumpBalances;
//use visitors::dump_tx_hashes::DumpTxHashes;
use preamble::*;

use std::io::Write;

pub use address::Address;
pub use hash::Hash;
pub use header::BlockHeader;
pub use script::HighLevel;

fn initialize_logger() {
    Builder::new()
        .filter(None, LevelFilter::Info)
        .format(|buf, record| {
            let t = time::now();
            writeln!(buf, "{}.{:04} - {} - {}", time::strftime("%Y-%m-%d %H:%M:%S", &t).unwrap(), t.tm_nsec / 100_000, record.level(), record.args())
        })
        .init();
}

fn main() {
    initialize_logger();
    let chain = unsafe { BlockChain::read() };

    let mut balances = HashMap::new();
    {
        let mut balances_visitor = DumpBalances::new();
        let (_, _, _) = chain.walk(&mut balances_visitor).unwrap();
        for (address_tuple, balance) in &balances_visitor.balances {

            if *balance == 0 {
                continue;
            }
            let address = &address_tuple.0;
            balances.insert(address.to_owned(),balance.to_owned());
        }
    }

    let mut clusterizer_visitor = Clusterizer::new();
    let (_, _, _) = chain.walk(&mut clusterizer_visitor).unwrap();

    let mut writer = LineWriter::new(OpenOptions::new().write(true).create(true).truncate(true).open(Path::new("clusters_bal.csv.tmp")).unwrap());

    let mut clusters = clusterizer_visitor.clusters;
    clusters.finalize();
    info!("Exporting {} clusters and balances to CSV...", clusters.size());


    let mut clusters_balances = HashMap::new();
    for (address, tag) in &clusters.map {
        let cluster=clusters.parent[*tag];
        let balance = balances.get(address).unwrap_or(&0i64);
        *clusters_balances.entry(cluster).or_insert(0)+=balance;
    }

    for (address, tag) in &clusters.map {
        let cluster = clusters.parent[*tag];
        let cluster_balance = clusters_balances.get(&cluster).unwrap_or(&0i64);
        if cluster_balance > &0 {
            writer.write_all(format!("{},{},{},{}\n",
                                     address,
                                     cluster,
                                     balances.get(address).unwrap_or(&&0i64),
                                     cluster_balance
            ).as_bytes()
            ).unwrap();
        }
    }

    fs::rename(Path::new("clusters_bal.csv.tmp"), Path::new("clusters_bal.csv")).unwrap();
    info!("Exported {} clusters to CSV.", clusters.size());


}
