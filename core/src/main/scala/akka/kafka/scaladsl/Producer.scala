/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.scaladsl

import akka.kafka.ProducerMessage._
import akka.kafka.internal.{CustomProducerStage, ProducerStage}
import akka.kafka.{ConsumerMessage, ProducerSettings}
import akka.stream.ActorAttributes
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.{Done, NotUsed}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/**
 * Akka Stream connector for publishing messages to Kafka topics.
 */
object Producer {

  /**
   * The `plainSink` can be used for publishing records to Kafka topics.
   * The `record` contains a topic name to which the record is being sent, an optional
   * partition number, and an optional key and value.
   */
  def plainSink[K, V](settings: ProducerSettings[K, V]): Sink[ProducerRecord[K, V], Future[Done]] =
    Flow[ProducerRecord[K, V]].map(Message(_, NotUsed))
      .via(flow(settings))
      .toMat(Sink.ignore)(Keep.right)

  /**
   * The `plainSink` can be used for publishing records to Kafka topics.
   * The `record` contains a topic name to which the record is being sent, an optional
   * partition number, and an optional key and value.
   */
  def plainSink[K, V](
    settings: ProducerSettings[K, V],
    producer: KafkaProducer[K, V]
  ): Sink[ProducerRecord[K, V], Future[Done]] =
    Flow[ProducerRecord[K, V]].map(Message(_, NotUsed))
      .via(flow(settings, producer))
      .toMat(Sink.ignore)(Keep.right)

  /**
   * Sink that is aware of the [[ConsumerMessage#CommittableOffset committable offset]]
   * from a [[Consumer]]. It will commit the consumer offset when the message has
   * been published successfully to the topic.
   *
   * Note that there is a risk that something fails after publishing but before
   * committing, so it is "at-least once delivery" semantics.
   */
  def commitableSink[K, V](settings: ProducerSettings[K, V]): Sink[Message[K, V, ConsumerMessage.Committable], Future[Done]] =
    flow[K, V, ConsumerMessage.Committable](settings)
      .mapAsync(settings.parallelism)(_.message.passThrough.commitScaladsl())
      .toMat(Sink.ignore)(Keep.right)

  /**
   * Sink that is aware of the [[ConsumerMessage#CommittableOffset committable offset]]
   * from a [[Consumer]]. It will commit the consumer offset when the message has
   * been published successfully to the topic.
   *
   * Note that there is always a risk that something fails after publishing but before
   * committing, so it is "at-least once delivery" semantics.
   */
  def commitableSink[K, V](
    settings: ProducerSettings[K, V],
    producer: KafkaProducer[K, V]
  ): Sink[Message[K, V, ConsumerMessage.Committable], Future[Done]] =
    flow[K, V, ConsumerMessage.Committable](settings, producer)
      .mapAsync(settings.parallelism)(_.message.passThrough.commitScaladsl())
      .toMat(Sink.ignore)(Keep.right)

  /**
   * Publish records to Kafka topics and then continue the flow. Possibility to pass through a message, which
   * can for example be a [[ConsumerMessage.CommittableOffset]] or [[ConsumerMessage.CommittableOffsetBatch]] that can
   * be committed later in the flow.
   */
  def flow[K, V, PassThrough](settings: ProducerSettings[K, V]): Flow[Message[K, V, PassThrough], Result[K, V, PassThrough], NotUsed] = {
    val flow = Flow.fromGraph(new ProducerStage[K, V, PassThrough](
      settings.closeTimeout,
      closeProducerOnStop = true,
      () => settings.createKafkaProducer()
    )).mapAsync(settings.parallelism)(identity)

    if (settings.dispatcher.isEmpty) flow
    else flow.withAttributes(ActorAttributes.dispatcher(settings.dispatcher))
  }

  /**
   * Publish records to Kafka topics and then continue the flow. Possibility to pass through a message, which
   * can for example be a [[ConsumerMessage.CommittableOffset]] or [[ConsumerMessage.CommittableOffsetBatch]] that can
   * be committed later in the flow.
   */
  def flow[K, V, PassThrough](
    settings: ProducerSettings[K, V],
    producer: KafkaProducer[K, V]
  ): Flow[Message[K, V, PassThrough], Result[K, V, PassThrough], NotUsed] = {
    val flow = Flow.fromGraph(new ProducerStage[K, V, PassThrough](
      closeTimeout = settings.closeTimeout,
      closeProducerOnStop = false,
      producerProvider = () => producer
    )).mapAsync(settings.parallelism)(identity)

    if (settings.dispatcher.isEmpty) flow
    else flow.withAttributes(ActorAttributes.dispatcher(settings.dispatcher))
  }

  def flow[E, PassThrough](
    settings: ProducerSettings[String, Array[Byte]],
    producer: KafkaProducer[String, Array[Byte]],
    producerRecord: E => ProducerRecord[String, Array[Byte]]
  ): Flow[(Iterable[E], PassThrough), PassThrough, NotUsed] = {
    val flow = Flow.fromGraph(new CustomProducerStage[E, PassThrough](
      closeTimeout = settings.closeTimeout,
      closeProducerOnStop = false,
      producerProvider = () => producer,
      producerRecordProvider = producerRecord
    )).mapAsync(settings.parallelism)(identity)

    if (settings.dispatcher.isEmpty) flow
    else flow.withAttributes(ActorAttributes.dispatcher(settings.dispatcher))
  }

}
