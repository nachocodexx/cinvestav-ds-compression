package mx.cinvestav

import fs2.io.file.Files
import cats.data.Kleisli
import cats.effect.kernel.Resource
import cats.implicits._
import cats.effect.{ExitCode, IO, IOApp}
import fs2.Stream
import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import org.apache.commons.compress.compressors.{CompressorInputStream, CompressorStreamFactory}
import fs2.io.{readInputStream, readOutputStream, writeOutputStream}

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.file.{Paths, StandardOpenOption, Files => FFF}
import mx.cinvestav.config.DefaultConfig
import org.http4s.client.Client
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.headers.`Content-Type`
import org.http4s.multipart.{Multipart, Part}
import org.typelevel.ci.CIString

import java.util.concurrent.{Executor, Executors}
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.global
// HTTP4s
import org.http4s.server.blaze._
import org.http4s.server._
import org.http4s._
import org.http4s.implicits._
import org.http4s.dsl.io._
import org.http4s.circe.CirceEntityCodec._
import org.http4s.circe._
// PureConfig
import pureconfig._
import pureconfig.generic.auto._
import mx.cinvestav.Compressing._
//
import scala.language.postfixOps
import scala.concurrent.duration._

object Application extends IOApp{
  val es = Executors.newScheduledThreadPool(10)
  val ec = ExecutionContext.fromExecutorService(es)
//  case class CompressNodeParameters(nodeId:String,createdAt:Long,file:Array[Byte],filename:String)
  case class NodeHealthCheckResponse(nodeId:String,issuedAt:Long,message:String,status:Int)
  case class CompressionNodeResponse(nodeId:String,issuedAt:Long,nodes:List[String])
  val clientBuilder: Resource[IO, Client[IO]] = BlazeClientBuilder[IO](global).resource

  def buildStorageNodeReq(url:String,filename:String,file:File) = {
    val body = Multipart[IO](
      Vector(
        Part.fileData("upload",file))
    )

    Request[IO](Method.POST,
      Uri.unsafeFromString(url))
      .withEntity(body)
      .withHeaders(
        Headers(
          Header.Raw(CIString("filename"),filename)
        ).headers.concat(body.headers.headers)
      )
      .pure[IO]
  }

  def services()(implicit C:DefaultConfig): HttpRoutes[IO] = HttpRoutes.of[IO]{
    case GET -> Root /"health-check" =>
      IO.realTime.flatMap{ issuedAt=>
      Ok(NodeHealthCheckResponse(C.nodeId,issuedAt.toSeconds,"I'm Ok :)",1))
      }
    case req@POST -> Root =>
      req.decode[Multipart[IO]]{ m=>
        val compressionExt = compressingX.getCompressionExt(C.compressingAlgorithm)
        val filename       = req.headers.get(CIString("filename")).map(_.head.value).getOrElse("sample")
        val fullFilename   = s"$filename.$compressionExt"

        val nodes          = req.headers.get(CIString("nodes")).map(_.head.value).getOrElse("http://localhost:6666")
          .split(',').toList

        val streams = Stream.emits(m.parts)
          .covary[IO]
          .flatMap{ part=>
            compressingX
              .compress(fullFilename,part.body)
          }
        val sendToNodes =IO.sleep(1 seconds)
          .flatMap(_=>streams.compile.drain)
          .flatMap{_=>
            val compressedFile = new File(s"${C.storagePath}/$fullFilename")
            println(compressedFile)
            val requestsIO    = nodes.traverse(buildStorageNodeReq(_,filename,compressedFile))
            requestsIO.flatMap{ requests=>
              clientBuilder.use{ client=>
                val responses = requests.traverse(client.expect[Json])
                responses.flatTap(IO.println)
              }
            }
          }

        IO.realTime.flatMap{issuedAt=>
          Ok(CompressionNodeResponse(C.nodeId,issuedAt.toSeconds,nodes))
            .flatMap{x=>
              sendToNodes
                .startOn(ec)
                .map(_=>x)
            }
        }
      }
  }

  def httpApp()(implicit C:DefaultConfig): Kleisli[IO, Request[IO], Response[IO]] =Router(
    s"/${C.nodeId}" -> services()
  ).orNotFound



  def runServer()(implicit C:DefaultConfig): IO[Nothing] =
    BlazeServerBuilder[IO](global)
      .bindHttp(C.port,C.host)
      .withHttpApp(httpApp())
      .resource
      .use{_=>IO.never}


  override def run(args: List[String]): IO[ExitCode] = {
    ConfigSource.default.load[DefaultConfig] match {
      case Left(value) =>
        println(value)
        IO.unit.as(ExitCode.Error)
      case Right(cfg) =>
        runServer()(cfg)
        .as(ExitCode.Success)
    }
  }
}
