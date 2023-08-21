import { Message, Stan } from 'node-nats-streaming';
import { Subjects } from './subjects';

interface Event {
  subject: Subjects;
  data: any;
}

abstract class Listener<T extends Event> {
  //----------------- abstract properties -----------------//
  // name of the channel this listener is listening to
  abstract subject: T['subject'];
  // name of the queue group this listener is part of
  abstract queueGroupName: string;

  //----------------- normal properties -----------------//
  // pre-initialized property
  private client: Stan;
  // number of seconds this listener has to ack a message
  private ackWait = 5 * 1000; // 5 seconds

  constructor(client: Stan) {
    this.client = client;
  }

  // ----------- normal methods ----------- //
  subscriptionOptions() {
    return this.client
      .subscriptionOptions()
      .setDeliverAllAvailable() // send all history events when needed to reprocessed
      .setManualAckMode(true) // set manual ack mode to true
      .setAckWait(this.ackWait) // set ack wait to 5 seconds
      .setDurableName(this.queueGroupName); // create a durable subscription in NATS server to know which events have been processed
  }

  listen() {
    const subscription = this.client.subscribe(
      this.subject,
      this.queueGroupName,
      this.subscriptionOptions()
    );

    subscription.on('message', (msg: Message) => {
      console.log(`Message received: ${this.subject} / ${this.queueGroupName}`);

      const parsedData = this.parseMessage(msg);
      this.onMessage(parsedData, msg);
    });
  }

  parseMessage(msg: Message) {
    const data = msg.getData();

    return typeof data === 'string'
      ? JSON.parse(data)
      : JSON.parse(data.toString('utf8'));
  }

  // ----------- abstract methods ----------- //
  abstract onMessage(data: T['data'], msg: Message): void;
}

export { Listener };
