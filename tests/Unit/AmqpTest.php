<?php

namespace ComLaude\Amqp\Tests\Unit;

use ComLaude\Amqp\Amqp;
use ComLaude\Amqp\Tests\BaseTest;
use Mockery;

/**
 * @author David Krizanic <david.krizanic@comlaude.com>
 */
class AmqpTest extends BaseTest
{
    public function setUp(): void
    {
        parent::setUp();

        $this->properties = array_merge($this->properties, [
            'connect_options' => ['heartbeat' => 2],
            'queue' => 'test_amqp_facade',
            'bindings' => [
                [
                    'queue'    => 'test_amqp_facade',
                    'routing'  => 'example.route.facade',
                ],
            ],
            'timeout' => 1,
        ]);
    }

    public function tearDown(): void
    {
        $this->deleteEverything($this->properties);
        parent::tearDown();
    }

    public function testPublishAndConsume()
    {
        $this->createQueue($this->properties);
        $messageBody = 'Test message publish and consume';

        $mockedFacade = new Amqp;
        $messages = 5;
        for ($i = 0; $i < $messages; $i++) {
            $mockedFacade->publish('example.route.facade', $messageBody);
        }

        $counter = 0;
        $mockedFacade->consume(function ($message) use ($messageBody, $mockedFacade, &$counter) {
            $this->assertEquals($messageBody, $message->getBody());
            $mockedFacade->acknowledge($message);
            $counter++;
        });
        for ($i = 0; $i < $messages - 1; $i++) {
            $this->consumeNextMessage($this->properties);
        }

        $this->assertEquals($messages, $counter);
    }

    public function testPublishConsumeAndAcknowledge()
    {
        $this->properties = array_merge(
            $this->properties,
            [
                'queue' => 'test_amqp_facade_acknowledge',
                'bindings' => [
                    [
                        'queue'    => 'test_amqp_facade_acknowledge',
                        'routing'  => 'example.route.facade_acknowledge',
                    ],
                ],
            ]
        );
        $this->createQueue($this->properties);
        $messageBody = 'Test message publish and consume, acknowledge - first';
        $messageBody2 = 'Test message publish and consume, acknowledge - second';

        $mockedFacade = new Amqp;

        $mockedFacade->publish('example.route.facade_acknowledge', $messageBody);
        $mockedFacade->publish('example.route.facade_acknowledge', $messageBody2);

        $counter = 0;
        $mockedFacade->consume(function ($message) use ($messageBody, $messageBody2, &$counter, $mockedFacade) {
            if ($counter == 1) {
                $this->assertEquals($messageBody2, $message->getBody());
            } else {
                $this->assertEquals($messageBody, $message->getBody());
            }
            $mockedFacade->acknowledge($message);
            $counter++;
        });

        $this->assertEquals(2, $counter);
    }

    public function testPublishConsumeAndReject()
    {
        $this->properties = array_merge(
            $this->properties,
            [
                'queue' => 'test_amqp_facade_reject',
                'bindings' => [
                    [
                        'queue'    => 'test_amqp_facade_reject',
                        'routing'  => 'example.route.facade_reject',
                    ],
                ],
            ]
        );
        $this->createQueue($this->properties);
        $messageBody = 'Test message publish and consume, reject - first';
        $messageBody2 = 'Test message publish and consume, reject - second';

        $mockedFacade = new Amqp;
        $mockedFacade->publish('example.route.facade_reject', $messageBody);
        $mockedFacade->publish('example.route.facade_reject', $messageBody2);

        $counter = 0;
        $mockedFacade->consume(function ($message) use ($messageBody, $messageBody2, &$counter, $mockedFacade) {
            if ($counter % 2 == 1) {
                $this->assertEquals($messageBody2, $message->getBody());
            } else {
                $this->assertEquals($messageBody, $message->getBody());
            }
            $mockedFacade->reject($message, false);
            $counter++;
        });

        $this->assertEquals(2, $counter);
    }

    public function testEnableDisablePublishing()
    {
        $this->properties = array_merge(
            $this->properties,
            [
                'queue' => 'test_amqp_facade_disable',
                'bindings' => [
                    [
                        'queue'    => 'test_amqp_facade_disable',
                        'routing'  => 'example.route.facade_disable',
                    ],
                ],
            ]
        );
        $this->createQueue($this->properties);
        $messageBody = 'Test message publish, disable and consume - first';
        $messageBody2 = 'Test message publish, disable and consume - second';
        $messageBody3 = 'Test message publish, disable and consume - third';

        $mockedFacade = new Amqp;
        $mockedFacade->publish('example.route.facade_disable', $messageBody);
        $mockedFacade->disable();
        $mockedFacade->publish('example.route.facade_disable', $messageBody2);
        $mockedFacade->enable();
        $mockedFacade->publish('example.route.facade_disable', $messageBody3);

        $counter = 0;
        $mockedFacade->consume(function ($message) use ($messageBody, $messageBody3, &$counter, $mockedFacade) {
            if ($counter % 2 == 1) {
                $this->assertEquals($messageBody3, $message->getBody());
            } else {
                $this->assertEquals($messageBody, $message->getBody());
            }
            $mockedFacade->reject($message, false);
            $counter++;
        });

        // The original consumer is still attached so just force it to get triggered again
        $this->consumeNextMessage($this->properties);

        $this->assertEquals(2, $counter);
    }

    public function testRequestNotAccepted()
    {
        $mockedFacade = new Amqp;
        $startTime = microtime(true);
        $mockedFacade->request(
            'example.route',
            ['message1', 'message2'],
            function ($message) {
                return null;
            }
        );
        $doneTime = microtime(true) - $startTime;
        $this->assertGreaterThan(0.5, $doneTime);
        $this->assertLessThan(1, $doneTime);
    }

    public function testRequestWithResponseNotAccepted()
    {
        $mockedFacade = new Amqp;
        $startTime = microtime(true);
        $this->assertNull($mockedFacade->requestWithResponse(
            'example.route.1',
            'message1'
        ));
        $doneTime = microtime(true) - $startTime;
        $this->assertGreaterThan(0.5, $doneTime);
        $this->assertLessThan(1, $doneTime);
    }

    public function testRequestWithResponseHandler()
    {
        $response = 'this worked!';
        $route = 'example.route.2';
        $message = 'message2';
        $mockedResponseMessage = Mockery::mock('PhpAmqpLib\Message\AMQPMessage[getBody]');
        $mockedResponseMessage->shouldReceive('getBody')->once()->andReturn($response);
        $mockedFacade = Mockery::mock('ComLaude\Amqp\Amqp[request]');
        $mockedFacade->shouldReceive('request')->once()->with(
            $route,
            [$message],
            Mockery::any(),
            [],
        )->andReturnUsing(function ($route, $messages, $callback, $properties) use ($mockedResponseMessage) {
            $callback($mockedResponseMessage);
        });
        $this->assertEquals($response, $mockedFacade->requestWithResponse(
            $route,
            $message
        ));
    }

    public function testCount()
    {
        $mockedFacade = new Amqp;
        $queue = 'test_amqp_facade_count';

        $this->properties = array_merge(
            $this->properties,
            [
                'queue' => $queue,
                'bindings' => [
                    [
                        'queue'    => $queue,
                        'routing'  => 'example.route.count',
                    ],
                ],
            ]
        );
        $this->createQueue($this->properties);
        $this->assertEquals(['messages' => 0, 'consumers' => 0], $mockedFacade->count($queue));

        $mockedFacade->publish('example.route.count', 'message');
        $this->assertEquals(['messages' => 1, 'consumers' => 0], $mockedFacade->count($queue));
    }
}
