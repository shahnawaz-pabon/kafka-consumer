const express = require("express");
const socketio = require("socket.io");
const http = require("http");
const cors = require("cors");
// const port = 3002;
const PORT = process.env.PORT || 5000;
const app = express();
const { Kafka } = require("kafkajs");

/**
 * Kafka consumer
 */

run().then(
  () => console.log("Done"),
  (err) => console.log(err)
);

async function run() {
  const kafka = new Kafka({ brokers: ["192.168.31.108:9092"] });
  // If you specify the same group id and run this process multiple times, KafkaJS
  // won't get the events. That's because Kafka assumes that, if you specify a
  // group id, a consumer in that group id should only read each message at most once.
  const consumer = kafka.consumer({ groupId: "email-send-test-0" });

  await consumer.connect();

  await consumer.subscribe({ topic: "email-send-test", fromBeginning: true });
  await consumer.run({
    eachMessage: async (data) => {
      console.log("data");
      console.log(data.message.value.toString("utf8"));
      // console.log(socket);

      io.sockets.emit(
        "MESSAGE_REQUEST_ACCEPTED",
        data.message.value.toString("utf8")
      );
    },
  });
}

/**
 * End of Kafka consumer
 */

/**
 * Socket Server
 */

const server = http.createServer(app);
const io = socketio(server, {
  cors: {
    origin: "*",
    // methods: ["GET", "POST"],
  },
});
app.use(cors());
io.on("connection", (socket) => {
  console.log("User has connected: " + socket.id);
  io.emit("SEND_NOTIFICATION", {
    message: "Chat connected",
    id: socket.id,
  });
  socket.on("hello", (arg) => {
    console.log(arg); // world
  });
  socket.on("disconnect", () => {
    console.log("User has disconnected.");
  });
});

server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
