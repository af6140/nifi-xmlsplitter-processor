package com.entertainment.nifi.processor.util;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.xml.namespace.QName;
import javax.xml.stream.*;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

/**
 * Created by dwang on 4/17/17.
 */
public class XMLSplitByCountUtil {

    private InputStream inputStream;

    private XMLInputFactory xmlInputFactory;
    private XMLOutputFactory xmlOutputFactory;
    private int splitDepth, splitCount;

    private String header;
    private String footer;

    public static String PREFIX="nifi_xmlsplitter";
    public static String SUFFIX="tmp.xml";
    public static String LINE_SEPARATOR=System.getProperty("line.separator");

    public XMLSplitByCountUtil(InputStream inputStream, int splitDepth , int splitCount, String header, String footer){
        this.splitDepth = splitDepth;
        this.splitCount =splitCount;
        this.inputStream = inputStream;
        this.xmlInputFactory = XMLInputFactory.newInstance();
        this.xmlOutputFactory = XMLOutputFactory.newInstance();
        this.xmlOutputFactory.setProperty("javax.xml.stream.isRepairingNamespaces"
                , Boolean.FALSE);
        this.header=header;
        this.footer=footer;
    }
    public final List<File> split(){
        List<File> splitFiles =new LinkedList<File>();
        List<OutputStream> openedStreams =new LinkedList<OutputStream>();
        XMLEventReader xmlEventReader=null;
        int depth=-1;
        try {
            xmlEventReader = xmlInputFactory.createXMLEventReader(this.inputStream);
            int count =0;
            File tmpFile=null;
            OutputStream outputStream =null;
            while (true) {
                XMLEvent event = xmlEventReader.nextEvent();
                if (event.getEventType() == XMLStreamConstants.START_ELEMENT) {
                    StartElement startElement = event.asStartElement();
                    depth++ ;
                    System.out.println("\nStart element: "+ startElement.getName().toString());
                    if(depth==0){
                        Writer writer=new StringWriter();
                        // this is the root element, get namespace
                        Iterator it = startElement.getNamespaces();
                        while(it.hasNext()) {
                            Object o = it.next();
                            writer.write(o.toString()+"\n");
                        }
                        System.out.println("XML namesapce: \n"+writer.toString());
                    }
                    System.out.println("Current depth : " + depth +" current count :"+count + " splitDepth:"+ splitDepth + " splitCount: "+ splitCount);
                    if(count ==0 && depth==splitDepth) {
                        //create new file
                        Set<PosixFilePermission> permissions = PosixFilePermissions.fromString("rwxr-x---");
                        FileAttribute<Set<PosixFilePermission>> fileAttributes = PosixFilePermissions.asFileAttribute(permissions);
                        tmpFile = Files.createTempFile(PREFIX, SUFFIX, fileAttributes).toFile();
                        splitFiles.add(tmpFile);
                        System.out.println("########################### :" + tmpFile.toPath().toAbsolutePath().toString());
                        outputStream = new BufferedOutputStream(new FileOutputStream(tmpFile));
                        openedStreams.add(outputStream);
                        if(this.header!=null) {
                            outputStream.write(this.header.getBytes());
                            outputStream.write(LINE_SEPARATOR.getBytes());
                        }
                    }
                    if (depth == splitDepth) {
                        count++;
                        System.out.println("Current count: " + count);
                        writeNode(xmlEventReader, event, outputStream);
                        // we get duplicate for current element
                        depth--;
                    }
                    if (count == splitCount) {
                        //reset count
                        count = 0;

                        if(this.footer!=null) {
                            outputStream.write(LINE_SEPARATOR.getBytes());
                            outputStream.write(this.footer.getBytes());
                        }
                        System.out.println("Close output stream###########################"+tmpFile.getName());
                        outputStream.flush();
                        if(outputStream!=null) {
                            try {
                                outputStream.close();
                            }catch(Exception e){

                            }
                        }
                    }
                }
                if (event.getEventType() == XMLStreamConstants.END_ELEMENT){
                    EndElement endElement=event.asEndElement();
                    depth--;
                    System.out.println("EndElement:"+endElement.getName().toString());
                    System.out.println("Current depth : " + depth +" current count :"+count + " splitDepth:"+ splitDepth + " splitCount: "+ splitCount);

                }

                if(event.getEventType()==XMLStreamConstants.END_DOCUMENT) {
                    //not finishing count
                    if(count < splitCount && count!=0) {
                        if(this.footer!=null) {
                            outputStream.write(File.separatorChar);
                            outputStream.write(this.footer.getBytes());
                        }
                        outputStream.flush();
                        if(outputStream!=null) {
                            System.out.println("Close output stream2###########################"+tmpFile.getName());
                            try {
                                outputStream.close();
                            }catch(Exception e){

                            }
                        }
                     }

                    break;
                }
            }
        } catch (XMLStreamException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(xmlEventReader!=null) {
                try {
                    xmlEventReader.close();
                }catch (Exception e){}
            }
        }
        return splitFiles;
     }

    protected void writeNode(XMLEventReader xmlEventReader, XMLEvent startEvent, OutputStream outputStream) {

        StartElement element = startEvent.asStartElement();
        QName name = element.getName();

        int stack = 1;
        XMLEventWriter writer =null;
        try {
            writer = this.xmlOutputFactory.createXMLEventWriter(outputStream);
            writer.add(element);
            while (true) {
                XMLEvent event = xmlEventReader.nextEvent();
                if (event.isStartElement()
                        && event.asStartElement().getName().equals(name))
                    stack++;
                if (event.isEndElement()) {
                    EndElement end = event.asEndElement();
                    if (end.getName().equals(name)) {
                        stack--;
                        if (stack == 0) {
                            writer.add(event);
                            break;
                        }
                    }
                }
                writer.add(event);
            }
            writer.close();
        } catch (XMLStreamException e) {
            e.printStackTrace();
        }finally {
            if(writer!=null) {
                try {
                    writer.close();
                } catch (Exception e) {

                }
            }
        }

    }
}
