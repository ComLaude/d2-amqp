<?php

namespace ComLaude\Amqp;

use Closure;
use PhpAmqpLib\Message\AMQPMessage;

/**
 * @author David Krizanic <david.krizanic@comlaude.com>
 */
class Amqp
{
    /**
     * Flag for globally disabling message publishing
     */
    private static $publishingEnabled = true;

    /**
     * Globally disables publishing messages
     */
    public function disable()
    {
        self::$publishingEnabled = false;
    }

    /**
     * Globally enables publishing messages
     */
    public function enable()
    {
        self::$publishingEnabled = true;
    }

    /**
     * Check if globally enabled
     */
    public function isEnabled()
    {
        return self::$publishingEnabled;
    }

    /**
     * Publishes a message to the queue
     *
     * @param string $route
     * @param string $message
     * @param array $properties
     * @param array $messageProperties
     */
    public function publish(string $route, string $message, array $properties = [], array $messageProperties = [])
    {
        if (! $this->isEnabled()) {
            return;
        }
        $message = new AMQPMessage($message, $messageProperties);
        AmqpFactory::create(array_merge($properties, [
            'queue' => 'publisher',
        ]))->publish($route, $message);
    }

    /**
     * Publishes a persistent message to the queue
     *
     * @param string $route
     * @param string $message
     * @param array $properties
     */
    public function publishPersistent(string $route, string $message, array $properties = [])
    {
        if (! $this->isEnabled()) {
            return;
        }
        return $this->publish($route, $message, $properties, ['delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT]);
    }

    /**
     * Adds a message received handler to a queue
     *
     * @param Closure $callback
     * @param array $properties
     * @throws Exception\Configuration
     */
    public function consume(Closure $callback, array $properties = [])
    {
        AmqpFactory::create($properties)->consume($callback);
    }

    /**
     * Acknowledges a message
     *
     * @param AMQPMessage $message
     * @param array $properties
     */
    public function acknowledge(AMQPMessage $message, array $properties = [])
    {
        AmqpFactory::create($properties)->acknowledge($message);
    }

    /**
     * Rejects a message and requeues it if wanted (default: false)
     *
     * @param AMQPMessage $message
     * @param bool $requeue
     * @param array $properties
     */
    public function reject(AMQPMessage $message, bool $requeue = false, array $properties = [])
    {
        AmqpFactory::create($properties)->reject($message, $requeue);
    }

    /**
     * Publishes messages with routing key and awaits a response on a dedicated reply-to queue
     * the responses for each message are passed to $callback to handle
     *
     * @param string $route
     * @param array $messages
     * @param Closure $callback
     * @param array $properties
     */
    public function request(string $route, array $messages, Closure $callback, array $properties = [])
    {
        // We override the queue away from default properties since we're going to
        // create an anonymous, exclusive queue to accept responses, we still permit
        // explicit overrides from the caller
        return AmqpFactory::createTemporary(array_merge($properties, [
            'exchange' => false,
            'queue' => false,
            'queue_passive' => false,
            'queue_durable' => false,
            'queue_exclusive' => true,
            'queue_auto_delete' => true,
            'queue_nowait' => false,
            'queue_properties' => ['x-ha-policy' => ['S', 'all'], 'x-queue-type' => ['S', 'classic']],
        ]))->request(
            $route,
            is_array($messages) ? $messages : [$messages],
            $callback,
            $properties
        );
    }

    /**
     * Publishes messages with routing key and awaits a response on a dedicated reply-to queue
     * the responses for each message are passed to $callback to handle
     *
     * @param string $route
     * @param string $message
     * @param array $properties
     */
    public function requestWithResponse(string $route, string $message, array $properties = [])
    {
        $response = null;
        $this->request(
            $route,
            [$message],
            function ($message) use (&$response) {
                $response = $message->getBody();
            },
            $properties,
        );
        return $response;
    }

    /**
     * Returns a count of messages, consumers on the queue
     *
     * @param string $queue
     * @param array $properties
     */
    public function count(string $queue, array $properties = [])
    {
        $count = AmqpFactory::createTemporary(array_merge($properties, [
            'queue' => $queue,
            'queue_passive' => true,
        ]))->declareQueue()->getQueue();
        return [
            'messages' => $count[1],
            'consumers' => $count[2],
        ];
    }
}
