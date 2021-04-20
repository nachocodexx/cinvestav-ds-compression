import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global
import mx.cinvestav.Compressing.compressingX
import mx.cinvestav.config.DefaultConfig
import org.apache.commons.compress.compressors.FileNameUtil
import pureconfig.ConfigSource
import fs2.io.file.Files
import io.circe.Json
import org.http4s.client.Client
import org.http4s.client.blaze.BlazeClientBuilder
import pureconfig.generic.auto._

import java.nio.file.Paths
import org.http4s.{Header, Headers, MediaType, Method, Request, Uri}
import org.http4s.circe._
import org.typelevel.ci.CIString
import org.http4s.multipart.{Multipart, Part}
import org.http4s.dsl.io._
import org.http4s.headers.{`Content-Disposition`, `Content-Type`}
import org.http4s.implicits._

import java.io.File
import java.net.URL
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
class CompressingXSpec extends munit.FunSuite {
  implicit val C = ConfigSource.default.load[DefaultConfig].getOrElse(DefaultConfig("",0,"","",""))
  private val PATH = "/home/nacho/Programming/Scala/cinvestav-ds-compressing/target/data"
  val es = Executors.newFixedThreadPool(2)
  val ec = ExecutionContext.fromExecutorService(es)
  val clientBuilder: Resource[IO, Client[IO]] = BlazeClientBuilder[IO](ec).resource
  val fileName ="607e2d10150fe24f6b2752c8"
  val storageNodeUrl = "http://10.0.0.1:6000/sn-00"

  test("Send file to node"){
//    val filepath = s"${C.storagePath}/$fileName.gz"
    val dispositionH = `Content-Disposition`("form-data",Map(CIString("name")->"upload",CIString("filename")->"HOLA.txt"))
    val filepath     = "/home/nacho/Programming/Scala/cinvestav-ds-compressing/target/data/compressed/HOLA.txt"
    val file         = new File(filepath)
    val fileS    = Files[IO].readAll(Paths.get(filepath),4096)
    val body = Multipart[IO](
      Vector(
//        Part.fileData("upload",filepath,fileS)
          Part.fileData("upload",file)
      )
    )

//    val rwq = POST
    val requestIO = Request[IO](
      Method.POST,
      Uri.unsafeFromString(storageNodeUrl)
    )
      .withEntity(body)
      .withHeaders(
        Headers(
          Header.Raw(CIString("filename"),fileName)
        ).headers.concat(body.headers.headers)
      )
//      .withHeaders(body.headers)

    val req = clientBuilder.use{ client=>
      client.expect[Json](requestIO)
    }.map(x=>println(x))

    req.unsafeRunSync()
//      .unsafeRunSync()
  }

  test("Compressing files"){
    val file          = Files[IO].readAll(Paths.get(s"$PATH/raw/sample.pdf"),4096)
    val compresedFile = compressingX.compress("test",file)
    compresedFile
//      .through(Files[IO].writeAll(Paths.get(s"$PATH/compressed/sample.pdf.gz")))
      .compile
      .drain
      .unsafeRunSync()
  }
  test("Uncompressing files"){
//    val path =
    val file = Files[IO].readAll(Paths.get(s"$PATH/compressed/sample.pdf.gz"),4096)
    compressingX
      .decompress(file)
      .through(Files[IO].writeAll(Paths.get(s"$PATH/decompressed/sample.pdf")))
      .compile
      .drain
      .unsafeRunSync()
//      .unsafeRunSync()
  }
  test("File extension"){
    val EXTENSION = ".gz"
    compressingX.getFileNameUtil(EXTENSION)
      .map(_.getCompressedFilename("test.pdf"))
      .map(assertEquals(_,"test.pdf.gz"))
      .unsafeRunSync()
  }

}
