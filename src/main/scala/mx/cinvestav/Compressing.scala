package mx.cinvestav
import org.apache.commons.compress.compressors.{CompressorStreamFactory, FileNameUtil}
import cats.implicits._
import fs2.Stream
import cats.effect.IO
import fs2.io.{readInputStream, toInputStream, writeOutputStream}
import fs2.io.file.Files
import mx.cinvestav.config.DefaultConfig

import java.io.{File, InputStream}
import java.nio.file.{Paths, StandardOpenOption, Files => NioFiles}
import java.util

trait CompressingX [F[_]]{
  def compress(filename:String,x:Stream[F,Byte])(implicit C:DefaultConfig):Stream[F,Byte]
  def decompress(file:Stream[F,Byte])(implicit C:DefaultConfig):Stream[F,Byte]
  def getFileNameUtil(ext:String):F[FileNameUtil]
  def getCompressionExt(x:String):String
}
object Compressing {

  val map: Map[String, String] =  Map[String,String](
    "gz"->"gz",
    "lz4-block" -> "lz4",
    "lz4-framed" -> "lz4"
  )
  implicit  val compressingX = new CompressingX[IO] {
    override def getCompressionExt(x:String): String = map.getOrElse(x,"gz")

    override def compress(filename:String,file:Stream[IO,Byte])(implicit C:DefaultConfig)
    : Stream[IO,
      Byte]
    = {
//      val compressionExt = getCompressionExt(C.compressingAlgorithm)
      val _filename      = filename
      val filePath       =s"${C.storagePath}/${_filename}"
      val CF             = new CompressorStreamFactory()
      val output         = NioFiles.newOutputStream(Paths.get(filePath),
        StandardOpenOption
        .CREATE_NEW)
      val _cOut          = CF.createCompressorOutputStream(C.compressingAlgorithm,output)
      val cOut           = writeOutputStream[IO](_cOut.pure[IO])
//      println(tempFile.getName)
//      tempFile.deleteOnExit()
      file
        .through(cOut)
        .onComplete(
          Files[IO]
          .readAll(Paths.get(filePath),4096)
        )
    }
    override def decompress(file:Stream[IO,Byte])(implicit C:DefaultConfig): Stream[IO,Byte] = {
      val input              = file.through(toInputStream)
      val CF                 = new CompressorStreamFactory()
      input
        .evalMap(CF.createCompressorInputStream(C.compressingAlgorithm,_).pure[IO])
        .flatMap(x=>readInputStream(x.asInstanceOf[InputStream].pure[IO],4096))
    }


    override def getFileNameUtil(ext: String): IO[FileNameUtil] = {
      val extensionMap = new util.HashMap[String,String]()
      extensionMap.put(s".pdf.$ext",".pdf")
      extensionMap.put(s".txt.$ext",".txt")
      extensionMap.put(s".png.$ext",".png")
      extensionMap.put(s".jpg.$ext",".png")
      extensionMap.put(s".$ext","")
      val fnu = new FileNameUtil(extensionMap,s".$ext")
      IO(fnu)
    }
  }
}
