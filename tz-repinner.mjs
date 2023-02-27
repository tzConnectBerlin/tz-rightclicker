import { readdir, readFile } from 'fs/promises'
import PinataClient from '@pinata/sdk';

//

const pinata_api_key = 'key';
const pinata_secret = 'secret';
const work_directory = 'hashes';

//

const pinata = new PinataClient(pinata_api_key, pinata_secret);

const process_file = async function(filename) {
  let file = await readFile(`${work_directory}/${filename}`, { encoding: 'utf-8' });
  let hashes = JSON.parse(file);
  for (let [hash, description] of hashes) {
    console.log('Pinning entry', description);
    let response = await pinata.pinByHash(hash, { name: description });
    console.log(`--> Pin job ${response.id} inserted, status: ${response.status}`);
  }
};

const run = async function() {

  let files = await readdir(work_directory);
  
  for (let filename of files) {
    if (filename.endsWith('.json')) {
      console.log('Processing file', filename);
      await process_file(filename);
    }
  }
};

run();