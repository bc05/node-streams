import { createWriteStream } from "fs";
import { MongoClient } from "mongodb";
import { Writable } from "stream";

const client = new MongoClient("mongodb://localhost:27017");
await client.connect();

const database = client.db("school");
const collection = database.collection("students");

// helper function
function getRandomArbitrary(min, max) {
  return Math.floor(Math.random() * (max - min) + min);
}

// insert data to test
// for (let i = 0; i < 500000; i++) {
//   const age = getRandomArbitrary(1, 70);
//   collection.insertOne({ name: `John-${i}`, age });
//   console.log(`Inserted ${i}`);
// }

const readableStream = collection.find({}).stream({
  transform: (student) => {
    return Buffer.from(JSON.stringify(student));
  },
});

const avaliableFileStudents = createWriteStream("avaliable-students.txt");
const notAvaliableFileStudents = createWriteStream(
  "not-avaliable-students.txt"
);

const writableStream = new Writable({
  write(chunk, _, callback) {
    const data = chunk.toString();
    const parsedData = JSON.parse(data);
    const formatedData = `${parsedData.name} - ${parsedData.age}\n`;

    console.log("writing to file...");
    console.log("--> handle data from mongodb:", parsedData._id);

    if (parsedData.age >= 18) {
      avaliableFileStudents.write(formatedData);
      callback();
      return;
    }

    notAvaliableFileStudents.write(formatedData);
    callback();
    return;
  },
});

readableStream.pipe(writableStream);
