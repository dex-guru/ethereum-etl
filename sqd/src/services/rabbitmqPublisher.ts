import * as amqp from 'amqplib';
import {AMQP_CONFIG} from '../config';

export interface BatchMessage {
    type: 'block';
    number: number;
}

export class RabbitMQPublisher {
    private connection: any = null;
    private channel: any = null;
    private isConnected: boolean = false;

    async connect(): Promise<void> {
        if (!AMQP_CONFIG.ENABLED) {
            console.log('AMQP publishing is disabled');
            return;
        }

        try {
            console.log(`Connecting to AMQP at ${AMQP_CONFIG.URL}...`);
            this.connection = await amqp.connect(AMQP_CONFIG.URL);
            this.channel = await this.connection.createChannel();
            
            // Ensure the queue exists
            await this.channel.assertQueue(AMQP_CONFIG.QUEUE_NAME, {
                durable: true
            });
            
            this.isConnected = true;
            console.log(`Connected to AMQP, queue: ${AMQP_CONFIG.QUEUE_NAME}`);
            
            // Handle connection errors
            this.connection.on('error', (err: Error) => {
                console.error('AMQP connection error:', err);
                this.isConnected = false;
            });
            
            this.connection.on('close', () => {
                console.log('AMQP connection closed');
                this.isConnected = false;
            });
            
        } catch (error) {
            console.error('Failed to connect to AMQP:', error);
            this.isConnected = false;
            throw error;
        }
    }

    async publishBatchMessage(firstBlock: number, lastBlock: number, routingKey?: string): Promise<void> {
        if (!AMQP_CONFIG.ENABLED || !this.isConnected || !this.channel) {
            return;
        }

        const message: BatchMessage[] = [
            { type: 'block', number: firstBlock },
            { type: 'block', number: lastBlock }
        ];

        try {
            const messageBuffer = Buffer.from(JSON.stringify(message));
            
            if (AMQP_CONFIG.EXCHANGE_NAME && AMQP_CONFIG.EXCHANGE_NAME.trim() !== '') {
                // Publish to exchange with routing key
                await this.channel.publish(
                    AMQP_CONFIG.EXCHANGE_NAME, 
                    routingKey || AMQP_CONFIG.ROUTING_KEY, 
                    messageBuffer, 
                    { persistent: true, contentType: "application/json" }
                );
                console.log(`Published batch message to exchange ${AMQP_CONFIG.EXCHANGE_NAME} with routing key "${routingKey || AMQP_CONFIG.ROUTING_KEY}": [${firstBlock}, ${lastBlock}]`);
            } else {
                // Send directly to queue
                await this.channel.sendToQueue(AMQP_CONFIG.QUEUE_NAME, messageBuffer, {
                    persistent: true,
                    contentType: "application/json"
                });
                console.log(`Published batch message to queue ${AMQP_CONFIG.QUEUE_NAME}: [${firstBlock}, ${lastBlock}]`);
            }
        } catch (error) {
            console.error('Failed to publish batch message:', error);
            // Don't throw the error to avoid breaking the main processing flow
        }
    }

    async close(): Promise<void> {
        if (!AMQP_CONFIG.ENABLED) {
            return;
        }

        try {
            if (this.channel) {
                await this.channel.close();
                this.channel = null;
            }
            if (this.connection) {
                await this.connection.close();
                this.connection = null;
            }
            this.isConnected = false;
            console.log('AMQP connection closed');
        } catch (error) {
            console.error('Error closing AMQP connection:', error);
        }
    }

    get connected(): boolean {
        return this.isConnected;
    }
}

// Singleton instance
export const rabbitmqPublisher = new RabbitMQPublisher(); 