package akka.kafka.internal

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
  * INTERNAL API
  */
private[kafka] class CustomProducerStage[T, P](
  closeTimeout: FiniteDuration, closeProducerOnStop: Boolean,
  producerProvider: () => KafkaProducer[String, Array[Byte]],
  producerRecordProvider: T => ProducerRecord[String, Array[Byte]]
)
  extends GraphStage[FlowShape[(Iterable[T], P), Future[P]]] {

  private val in = Inlet[(Iterable[T], P)]("input")
  private val out = Outlet[Future[P]]("output")
  override def shape: FlowShape[(Iterable[T], P), Future[P]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    val producer = producerProvider()
    val logic = new GraphStageLogic(shape) with StageLogging {
      val awaitingConfirmation = new AtomicInteger(0)
      @volatile var inIsClosed = false

      var completionState: Option[Try[Unit]] = None

      def checkForCompletion() = {
        if (isClosed(in) && awaitingConfirmation.get == 0) {
          completionState match {
            case Some(Success(_)) => completeStage()
            case Some(Failure(ex)) => failStage(ex)
            case None => failStage(new IllegalStateException("Stage completed, but there is no info about status"))
          }
        }
      }

      val checkForCompletionCB = getAsyncCallback[Unit] { _ =>
        checkForCompletion()
      }

      setHandler(out, new OutHandler {
        override def onPull() = {
          tryPull(in)
        }
      })

      setHandler(in, new InHandler {
        override def onPush() = {
          val (records, passThrough) = grab(in)
          var recordsToSend = records.size
          val r = Promise[P]

          records.map(producerRecordProvider).foreach { pr =>
            producer.send(pr, new Callback {
              override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
                if (exception == null) {
                  recordsToSend = recordsToSend - 1
                  if (recordsToSend == 0) r.success(passThrough)
                } else {
                  r.failure(exception)
                }

                if (awaitingConfirmation.decrementAndGet() == 0 && inIsClosed)
                  checkForCompletionCB.invoke(())
              }
            })
            awaitingConfirmation.incrementAndGet()
          }

          push(out, r.future)
        }

        override def onUpstreamFinish() = {
          inIsClosed = true
          completionState = Some(Success(()))
          checkForCompletion()
        }

        override def onUpstreamFailure(ex: Throwable) = {
          inIsClosed = true
          completionState = Some(Failure(ex))
          checkForCompletion()
        }
      })

      override def postStop() = {
        log.debug("Stage completed")

        if (closeProducerOnStop) {
          try {
            producer.flush()
            producer.close(closeTimeout.toMillis, TimeUnit.MILLISECONDS)
            log.debug("Producer closed")
          }
          catch {
            case NonFatal(ex) => log.error(ex, "Problem occurred during producer close")
          }
        }

        super.postStop()
      }
    }

    logic
  }
}
