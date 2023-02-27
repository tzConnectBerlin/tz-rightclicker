import { writeFile } from 'fs/promises';

import fetch from 'node-fetch'
import AbortController from 'abort-controller'

import { createRequire } from 'module'
const require = createRequire(import.meta.url);
const { Pool } = require('pg');

//

const collection_name = "tezos";
const token_quepasa_schema = "tezos";
const ipfs_gateway = "yourpinata.mypinata.cloud/ipfs";
const burn_address = 'tz1burnburnburnburnburnburnburjAYjjX';
const db_connection = {
	"host": "localhost",
	"port": 5432,
	"user": "tezos",
	"password": "tezos",
	"database": "tezos"
};
const work_directory = "hashes";

//

let pool = new Pool(db_connection);

const SafeFetcher = function({ timeout_ms, minperiod_ms }) {
	let lastFetch = 0;
	return async function(url) {
    console.log('Fetching url', url);
		while (true) {
			let to_wait_ms = minperiod_ms - (Date.now() - lastFetch);
			if (to_wait_ms > 0) {
				await new Promise(_ => setTimeout(_, to_wait_ms));
			}
			let abortController = new AbortController();
			let watchdog = setTimeout(() => {
				abortController.abort();
			}, timeout_ms);
			try {
				const response = await fetch(url, {signal: abortController.signal});
				lastFetch = Date.now();
				if (response.status === 200) {
					return response;
				} else {
					let msg = await response.text();
					throw new Error(msg);
				}
			} catch (error) {
				console.log("Error while fetching data...", error);
				console.log("Retrying...");
			} finally {
				clearTimeout(watchdog);
			}		
		}
	}
}

const TOKEN_METADATA_EXTRACTOR_SQL = `SELECT DISTINCT
  token.assets_token_id AS token_id,
	token_info.assets_bytes AS metadata_hex
	FROM "${token_quepasa_schema}"."storage.token_metadata_live" AS token
	INNER JOIN "${token_quepasa_schema}"."storage.token_metadata.token_info" AS token_info
	ON token_info.tx_context_id = token.tx_context_id
	AND token_info.token_metadata_id = token.id
	INNER JOIN "${token_quepasa_schema}"."storage.ledger_live" AS ledger
	ON ledger.idx_assets_nat = token.assets_token_id
	AND ledger.assets_nat > 0
	WHERE ledger.idx_assets_address <> '${burn_address}'
	ORDER BY token.assets_token_id ASC`;

const CONTRACT_METADATA_EXTRACTOR_SQL = `SELECT
	idx_string AS field_name,
	bytes AS metadata_hex
	FROM "${token_quepasa_schema}"."storage.metadata"
	ORDER BY idx_string`;

const token_metadata_extractor = async function() {
	let result = await pool.query(TOKEN_METADATA_EXTRACTOR_SQL, [ ]);
	return result.rows;
}

const contract_metadata_extractor = async function() {
	let result = await pool.query(CONTRACT_METADATA_EXTRACTOR_SQL, [ ]);
	return result.rows;
}

const hex_to_ascii = function(str1)
{
	var hex  = str1.toString();
	var str = '';
	for (var n = 0; n < hex.length; n += 2) {
		str += String.fromCharCode(parseInt(hex.substr(n, 2), 16));
	}
	return str;
}

const decode_rows = function(rows) {
	let decoded = rows.map((row) => {
		row.metadata = hex_to_ascii(row.metadata_hex);
		return row;
	});
	return decoded;
};

const strip_ipfs_link = function(raw) {
	if (raw.slice(0,7) === "ipfs://") {
		let hash = raw.slice(7);
    if (/^Qm[1-9A-HJ-NP-Za-km-z]{44}$/.test(hash)) {
      return hash;
    }
	}
  console.log('Not a valid IPFS hash..');
	return null;
};

const process_link = function(link_string, description, hashmap) {
	if (link_string) {
		let hash = strip_ipfs_link(link_string);
		if (hash && !hashmap.has(hash)) {
			hashmap.set(hash, description);
			console.log(`---> IPFS hash ${description} recorded..`);
			return hash;
		}
	}
	return null;
};

const run = async function() {
	let fetch_ipfs_internal = SafeFetcher({
		timeout_ms: 10000,
		minperiod_ms: 500
	});

	const fetch_ipfs = async function(hash) {
		let result = await fetch_ipfs_internal(`https://${ipfs_gateway}/${hash}`);
		try {
      return await result.json();
	  } catch (err) {
      console.error('Non-JSON document retrieved, or cannot parse json.');
      return null;
    }
  };

	let hashes = new Map();

	let contract_metadata_rows = await contract_metadata_extractor();
	console.log(`${contract_metadata_rows.length} contract metadata rows retrieved from db..`);
	contract_metadata_rows = decode_rows(contract_metadata_rows);

	for (let row of contract_metadata_rows) {
		let hash = process_link(row.metadata, `${collection_name}-contract_metadata_(${row.field_name})`, hashes);
		if (hash) { //don't ask, lol
			console.log(`Fetching IPFS metadata document for contract level metadata entry '${row.field_name}'..`);
			let metadata_json = await fetch_ipfs(hash);
			if (metadata_json) {
				console.log(`Metadata document for contract level metadata entry '${row.field_name}' retrieved - recording IPFS hashes..`);
				process_link(metadata_json.imageUri, `${collection_name}-contract_metadata_(${row.field_name})-image`, hashes);
			}
		}
	}

	let tokens = await token_metadata_extractor();
	console.log(`${tokens.length} tokens retrieved from db..`);
	tokens = decode_rows(tokens);

	for (let row of tokens) {
    console.log(`Processing token ${row.token_id}:\n`, row);
		let hash = process_link(row.metadata, `${collection_name}-token_${row.token_id}-descriptor`, hashes);
		if (hash) {
			console.log(`Fetching IPFS metadata document for token ${row.token_id}..`);
			let metadata_json = await fetch_ipfs(hash);
			if (metadata_json) {
				console.log(`Metadata document for token ${row.token_id} retrieved - recording IPFS hashes..`);
				process_link(metadata_json.artifactUri, `${collection_name}-token_${row.token_id}-artifact-main`, hashes);
				process_link(metadata_json.displayUri, `${collection_name}-token_${row.token_id}-artifact-display`, hashes);
				process_link(metadata_json.thumbnailUri, `${collection_name}-token_${row.token_id}-artifact-thumbnail`, hashes);
				for (let [format, i] of metadata_json.formats.entries()) {
					process_link(format.uri, `${collection_name}-token_${row.token_id}-artifact-${format.mimetype}-${i}`, hashes);
				}
			}
		} else {
			console.log(`No new IPFS link for token ${row.token_id}`);
		}
	}

	console.log(`Processing finished, writing hashes to file ${collection_name}_hashes.json`)
	let hash_array = [...hashes];
	await writeFile(`${work_directory}/${collection_name}_hashes.json`, JSON.stringify(hash_array, null, 4));
	console.log('kthxbye!');
}

run();
