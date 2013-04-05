package com.senseidb.ba.util;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * Taken from http://www.thoughtspark.org/node/53
 * @author Jeremy Whitlock
 *
 */
public class TarGzCompressionUtils {
  private static Logger logger = Logger.getLogger(TarGzCompressionUtils.class);
  /**
   * Creates a tar.gz file at the specified path with the contents of the
   * specified directory.
   * 
   * @param dirPath
   *          The path to the directory to create an archive of
   * @param archivePath
   *          The path to the archive to create
   * 
   * @throws IOException
   *           If anything goes wrong
   */
  public static void createTarGzOfDirectory(String directoryPath, String tarGzPath) throws IOException {
    FileOutputStream fOut = null;
    BufferedOutputStream bOut = null;
    GzipCompressorOutputStream gzOut = null;
    TarArchiveOutputStream tOut = null;

    try {
      fOut = new FileOutputStream(new File(tarGzPath));
      bOut = new BufferedOutputStream(fOut);
      gzOut = new GzipCompressorOutputStream(bOut);
      tOut = new TarArchiveOutputStream(gzOut);
      tOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
      addFileToTarGz(tOut, directoryPath, "");
    } finally {
      tOut.finish();

      tOut.close();
      gzOut.close();
      bOut.close();
      fOut.close();
    }
  }

  /**
   * Creates a tar entry for the path specified with a name built from the base
   * passed in and the file/directory name. If the path is a directory, a
   * recursive call is made such that the full directory is added to the tar.
   * 
   * @param tOut
   *          The tar file's output stream
   * @param path
   *          The filesystem path of the file/directory being added
   * @param base
   *          The base prefix to for the name of the tar file entry
   * 
   * @throws IOException
   *           If anything goes wrong
   */
  private static void addFileToTarGz(TarArchiveOutputStream tOut, String path, String base) throws IOException {
    File f = new File(path);
    String entryName = base + f.getName();
    TarArchiveEntry tarEntry = new TarArchiveEntry(f, entryName);

    tOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
    tOut.putArchiveEntry(tarEntry);

    if (f.isFile()) {
      IOUtils.copy(new FileInputStream(f), tOut);

      tOut.closeArchiveEntry();
    } else {
      tOut.closeArchiveEntry();

      File[] children = f.listFiles();

      if (children != null) {
        for (File child : children) {
          addFileToTarGz(tOut, child.getAbsolutePath(), entryName + "/");
        }
      }
    }
  }
  /** Untar an input file into an output file.

   * The output file is created in the output folder, having the same name
   * as the input file, minus the '.tar' extension. 
   * 
   * @param inputFile     the input .tar file
   * @param outputDir     the output directory file. 
   * @throws IOException 
   * @throws FileNotFoundException
   *  
   * @return  The {@link List} of {@link File}s with the untared content.
   * @throws ArchiveException 
   */
  public static List<File> unTar(final File inputFile, final File outputDir) throws FileNotFoundException, IOException, ArchiveException {

    logger.debug(String.format("Untaring %s to dir %s.", inputFile.getAbsolutePath(), outputDir.getAbsolutePath()));
    TarArchiveInputStream debInputStream = null;
    InputStream is = null;
     final List<File> untaredFiles = new LinkedList<File>();
      try {
        is = new GzipCompressorInputStream(new FileInputStream(inputFile)); 
       debInputStream = (TarArchiveInputStream) new ArchiveStreamFactory().createArchiveInputStream("tar", is);
      TarArchiveEntry entry = null; 
      while ((entry = (TarArchiveEntry)debInputStream.getNextEntry()) != null) {
          final File outputFile = new File(outputDir, entry.getName());
          if (entry.isDirectory()) {
            logger.debug(String.format("Attempting to write output directory %s.", outputFile.getAbsolutePath()));
              if (!outputFile.exists()) {
                logger.debug(String.format("Attempting to create output directory %s.", outputFile.getAbsolutePath()));
                  if (!outputFile.mkdirs()) {
                      throw new IllegalStateException(String.format("Couldn't create directory %s.", outputFile.getAbsolutePath()));
                  }
              } else {
                logger.error("The directory a;ready there. Deleting - " + outputFile.getAbsolutePath());
                FileUtils.deleteDirectory(outputFile);
              }
          } else {
            logger.debug(String.format("Creating output file %s.", outputFile.getAbsolutePath()));
            File directory = outputFile.getParentFile();
            if (!directory.exists()) {
              directory.mkdirs();
            }
            OutputStream outputFileStream = null;  
            try {
              outputFileStream = new FileOutputStream(outputFile); 
              IOUtils.copy(debInputStream, outputFileStream);
            } finally {
              IOUtils.closeQuietly(outputFileStream);
            }
          }
          untaredFiles.add(outputFile);
      }
      } finally {
        IOUtils.closeQuietly(debInputStream); 
        IOUtils.closeQuietly(is); 
      }
      return untaredFiles;
  }
  public static InputStream unTarOneFile(InputStream tarGzInputStream, final String filename) throws FileNotFoundException, IOException, ArchiveException {
    TarArchiveInputStream debInputStream = null;
    InputStream is = null;
      try {
        is = new GzipCompressorInputStream(tarGzInputStream); 
       debInputStream = (TarArchiveInputStream) new ArchiveStreamFactory().createArchiveInputStream("tar", is);
      TarArchiveEntry entry = null; 
      while ((entry = (TarArchiveEntry)debInputStream.getNextEntry()) != null) {
        if (entry.getName().contains(filename)) {
          ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
          IOUtils.copy(debInputStream, byteArrayOutputStream);
          return new ByteArrayInputStream( byteArrayOutputStream.toByteArray());
        }  
      }
      } finally {
        IOUtils.closeQuietly(debInputStream); 
        IOUtils.closeQuietly(is); 
      }
      return null;
  }
}
