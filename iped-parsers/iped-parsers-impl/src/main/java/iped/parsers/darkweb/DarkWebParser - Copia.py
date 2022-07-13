'''
Python parser example.
To use python parsers, first you must install JEP, see https://github.com/sepinf-inc/IPED/wiki/User-Manual#python-modules
Save your parser in 'parsers' folder. The class name must be equal to the script name without the extension. 
For more info about general parser api, see https://github.com/sepinf-inc/IPED/wiki/Contributing
'''

from hashlib import new
from typing import List
from org.apache.tika.sax import XHTMLContentHandler
from org.apache.tika.io import TikaInputStream
from org.apache.tika.io import TemporaryResources
from org.apache.tika.metadata import Metadata, Message, TikaCoreProperties
from org.apache.tika.extractor import EmbeddedDocumentExtractor
from org.apache.tika.mime import MediaType
from iped.properties import BasicProps, ExtraProperties
from iped.utils import EmptyInputStream
from iped.parsers.standard import StandardParser
import sqlite3

class DarkWebParser:
    '''
    Example of a python parser. This class must be thread safe.
    One way of achieving this is creating an immutable class i.e. do not create instance attributes.
    '''
    
    DARKWEB = "DARKWEB"
    DARKWEB_SITES_FOLDER = "DARKWEB-SITES"
    DARKWEB_PROFILES_FOLDER = "DARKWEB-PROFILES"
    DARKWEB_DB_MIME = "application/x-darkweb-db"
    DARKWEB_SITE_MIME = "application/x-darkweb-site"
    DARKWEB_PROFILE_MIME = "application/x-darkweb-profile"
    DARKWEB_TOPIC_MIME = "application/x-darkweb-topic"
    DARKWEB_MESSAGE_MIME = "application/x-darkweb-message"
    DARKWEB_ATTACHMENT_MIME = "application/x-darkweb-attachment"
    #SUPPORTED_TYPES = (DARKWEB_DB,DARKWEB_SITE,DARKWEB_PROFILE,DARKWEB_TOPIC,DARKWEB_MESSAGE,DARKWEB_ATTACHMENT)

    def getSupportedTypes(self, context):
        '''
        Returns:
            list of supported media types handled by this parser
        '''
        return ["application/x-darkweb-db"]

    # end getSupportedTypes
    
    # def getSupportedTypesQueueOrder(self):
        '''
        This method is optional. You only need to implement it if your parser
        needs to access other case items different from the item being processed.
        E.G. you want to access case multimedia files while processing a chat database
        to insert the attachments in the chats being decoded. To work, you need to
        process the database mediaType after the attachments, in a later queue,
        so the attachments will be already indexed and ready to be searched for.
        Default queue number is 0 (first queue) if not defined for a mediaType.
        
        Returns:
            dictionary mapping mediaTypes to a queue number
        '''    
        # return {"application/xxxxxx" : 1}

    '''
        Parses each item found in case of the supported types.
        
        Parameters:
        stream: java.io.InputStream
            the raw (binary) content of the file
        handler: org.xml.sax.ContentHandler
            the content handler where you should output parsed content
        metadata: org.apache.tika.metadata.Metadata
            the metadata object from where you can get basic properties and store new parsed ones.
        context: org.apache.tika.parser.ParseContext
            the parsing context from where you can get parsing configuration
            
    Raises
        IOException: java.io.IOException
            if the file stream being read throws an IOException
        SAXException: org.xml.sax.SAXException
            if there is an error when writing the parser output to the handler.
        TikaException: org.apache.tika.exception.TikaException
            you can throw a TikaException if the file being parsed is corrupted or not supported.
    '''
    # Start parsing the DARKWEB (root) and populate with its SITES (parseDarkWebDB)
    def parse(self, stream, handler, metadata, context):
        
        print("inside parse")

        xhtml = XHTMLContentHandler(handler, metadata)
        xhtml.startDocument()
        extractor = context.get(EmbeddedDocumentExtractor)
        tmpResources = TemporaryResources()

        try:
            
            tis = TikaInputStream.get(stream, tmpResources)
            tmpFilePath = tis.getFile().getAbsolutePath()
            #origFileName = metadata.get(TikaCoreProperties.RESOURCE_NAME_KEY)

            conDB = None
            # get connection to DARKWEB.db
            conDB = sqlite3.connect(tmpFilePath)
            if conDB:

                conDB.row_factory = sqlite3.Row

                # prepare DARKWEB.db parsed HTML
                #REMOVE
                xhtml.startElement("p")
                xhtml.characters("connected to DB")
                xhtml.endElement("p")

                #REMOVE
                xhtml.startElement("p")
                xhtml.characters("SITES:")
                xhtml.endElement("p")
                
                meta = Metadata()
                # create SITES folder under DB
                meta.set(ExtraProperties.EMBEDDED_FOLDER,"true")
                meta.set(ExtraProperties.PARENT_VIRTUAL_ID,self.DARKWEB)
                meta.set(ExtraProperties.ITEM_VIRTUAL_ID,self.DARKWEB_SITES_FOLDER)
                meta.set(TikaCoreProperties.RESOURCE_NAME_KEY,self.DARKWEB_SITES_FOLDER)
                extractor.parseEmbedded(EmptyInputStream(),handler,meta,False)
                # create PROFILES folder under DB
                meta.set(ExtraProperties.EMBEDDED_FOLDER,"true")
                meta.set(ExtraProperties.PARENT_VIRTUAL_ID,self.DARKWEB)
                meta.set(ExtraProperties.ITEM_VIRTUAL_ID,self.DARKWEB_PROFILES_FOLDER)
                meta.set(TikaCoreProperties.RESOURCE_NAME_KEY,self.DARKWEB_PROFILES_FOLDER)
                extractor.parseEmbedded(EmptyInputStream(),handler,meta,False)               

                # get the list of profiles
                numProfile = 0
                for row in conDB.execute("SELECT PROFILE_GUID,PROFILES.NAME AS PROFILE_NAME,PROFILES.SITE_GUID,SITES.NAME AS SITE_NAME FROM PROFILES LEFT JOIN SITES ON PROFILES.SITE_GUID=SITES.SITE_GUID"):
                    numProfile += 1

                    profileMetadata = Metadata()
                    profileMetadata.set(StandardParser.INDEXER_CONTENT_TYPE,self.DARKWEB_PROFILE_MIME)
                    #profileMetadata.set(BasicProps.HASCHILD,"true")
                    profileMetadata.set(ExtraProperties.ITEM_VIRTUAL_ID,row["PROFILE_GUID"])
                    profileMetadata.set(ExtraProperties.PARENT_VIRTUAL_ID,self.DARKWEB_PROFILES_FOLDER)
                    profileMetadata.set(TikaCoreProperties.RESOURCE_NAME_KEY,row["PROFILE_NAME"] + "@" + row["SITE_NAME"])
                    profileMetadata.set("DARKWEB:ProfileGUID",row["PROFILE_GUID"])
                    profileMetadata.set("DARKWEB:ProfileName",row["PROFILE_NAME"])
                    profileMetadata.set("DARKWEB:SiteGUID",row["SITE_GUID"])
                    profileMetadata.set("DARKWEB:SiteName",row["SITE_NAME"])
                    
                    #REMOVE
                    print("ProfileGUID = " + row["PROFILE_GUID"])
                    print("ProfileName = " + row["PROFILE_NAME"])

                    if (extractor.shouldParseEmbedded(profileMetadata)):
                        print("parsing embedded")
                        extractor.parseEmbedded(EmptyInputStream(),handler,profileMetadata,False)

                # end for (profiles)
                print("END FOR PROFILES")

                # get the list of sites
                numSite = 0
                for row in conDB.execute("SELECT * FROM SITES"):

                    numSite += 1
                    #REMOVE
                    print("numSite = " + str(numSite))

                    #REMOVE
                    xhtml.startElement("p")
                    xhtml.characters(row["NAME"])
                    xhtml.endElement("p")

                    siteMetadata = Metadata()
                    siteMetadata.set(StandardParser.INDEXER_CONTENT_TYPE,self.DARKWEB_SITE_MIME)
                    siteMetadata.set(BasicProps.HASCHILD,"true")
                    #siteMetadata.set(ExtraProperties.EMBEDDED_FOLDER,"true")
                    siteMetadata.set(ExtraProperties.ITEM_VIRTUAL_ID,row["SITE_GUID"])
                    siteMetadata.set(ExtraProperties.PARENT_VIRTUAL_ID,self.DARKWEB_SITES_FOLDER)#self.DARKWEB)
                    siteMetadata.set(TikaCoreProperties.RESOURCE_NAME_KEY,row["NAME"])
                    siteMetadata.set("DARKWEB:SiteGUID",row["SITE_GUID"])
                    siteMetadata.set("DARKWEB:SiteName",row["NAME"])
                    
                    #REMOVE
                    print("site_GUID = " + row["SITE_GUID"])
                    print("site_NAME = " + row["NAME"])

                    if (extractor.shouldParseEmbedded(siteMetadata)):
                        print("parsing embedded")
                        extractor.parseEmbedded(EmptyInputStream(),handler,siteMetadata,False)
                
                    self.parseDarkWebSite(conDB,EmptyInputStream(),handler,siteMetadata,context)

                # end for (sites)
                print("END FOR SITES")

                # populate parsed metadata to be shown as new item properties 
                metadata.add("DARKWEB:DBParsed", "True")
            else:
                xhtml.startElement("p")
                xhtml.characters("NOT connected to DB")
                xhtml.endElement("p")
                # populate parsed metadata to be shown as new item properties 
                metadata.add("DARKWEB:DBParsed", "False")

        except Exception as exc:
            raise exc

        finally:
            xhtml.endDocument()
            if conDB:
                conDB.close()
            # if tmpResources is used above you must close it
            tmpResources.close()

    # end parse

    """ def parseDarkWebProfile(self, conDB, stream, handler, metadata, context):
        return 0
    # end parseDarkWebProfile """

    # Parse the SITE and populate with its TOPICS
    def parseDarkWebSite(self, conDB, stream, handler, metadata, context):
        
        xhtml = XHTMLContentHandler(handler, metadata)
        xhtml.startDocument()
        
        # uncomment if used below
        #tmpResources = TemporaryResources()
        try:
            
            extractor = context.get(EmbeddedDocumentExtractor)
            """ tis = TikaInputStream.get(stream, tmpResources)
            tmpFilePath = tis.getFile().getAbsolutePath()
            origFileName = metadata.get(TikaCoreProperties.RESOURCE_NAME_KEY)

            # decode file contents and write the output of your parser to a xhtml document

            conDB = None
            #curDB = None
            
            # get connection to DARKWEB.db
            conDB = sqlite3.connect(tmpFilePath) """
            print("inside parseDarkWebSite")
            if conDB:

                print("connected to DB inside parseDarkWebSite")
                conDB.row_factory = sqlite3.Row

                # prepare DARKWEB.db parsed HTML
                xhtml.startElement("p")
                xhtml.characters("connected to DB")
                xhtml.endElement("p")

                xhtml.startElement("p")
                xhtml.characters("TOPICS:")
                xhtml.endElement("p")
                
                # get the list of topics for the site being processed
                site_GUID = metadata.get("DARKWEB:SiteGUID")
                print("site_GUID = " + str(site_GUID))

                numTopics = 0
                for row in conDB.execute("SELECT TOPIC_GUID,SITES.NAME AS SITE_NAME,PROFILES.NAME AS PROFILE_CREATOR_NAME,TITLE,ID,CATEGORIES.CATEGORIES AS CATEGORIES FROM TOPICS LEFT JOIN SITES ON TOPICS.SITE_GUID=SITES.SITE_GUID LEFT JOIN PROFILES ON TOPICS.PROFILE_CREATOR_GUID=PROFILES.PROFILE_GUID LEFT JOIN CATEGORIES ON TOPICS.CATEGORIES_ID=CATEGORIES.CATEGORIES_ID WHERE TOPICS.SITE_GUID='" + str(site_GUID) + "'"):

                    numTopics += 1
                    print("numTopics = " + str(numTopics))

                    xhtml.startElement("p")
                    xhtml.characters(row["TITLE"])
                    xhtml.endElement("p")

                    topicMetadata = Metadata()
                    topicMetadata.set(StandardParser.INDEXER_CONTENT_TYPE,self.DARKWEB_TOPIC_MIME)
                    topicMetadata.set(ExtraProperties.ITEM_VIRTUAL_ID,row["TOPIC_GUID"])
                    topicMetadata.set(ExtraProperties.PARENT_VIRTUAL_ID,site_GUID)
                    topicMetadata.set(BasicProps.HASCHILD,"true")
                    topicMetadata.set(TikaCoreProperties.RESOURCE_NAME_KEY,row["TITLE"])
                    topicMetadata.set("DARKWEB:TopicGUID",row["TOPIC_GUID"])
                    topicMetadata.set("DARKWEB:SiteGUID",site_GUID)
                    topicMetadata.set("DARKWEB:SiteName",row["SITE_NAME"])
                    topicMetadata.set("DARKWEB:TopicProfileCreatorName",row["PROFILE_CREATOR_NAME"])
                    topicMetadata.set("DARKWEB:TopicTitle",row["TITLE"])
                    topicMetadata.set("DARKWEB:TopicID",str(row["ID"]))
                    topicMetadata.set("DARKWEB:TopicCategories",row["CATEGORIES"])
                    
                    if (extractor.shouldParseEmbedded(topicMetadata)):
                        extractor.parseEmbedded(EmptyInputStream(),handler,topicMetadata,False)
               
                    #self.parseDarkWebTopic(conDB,EmptyInputStream(),handler,topicMetadata,context)

                # end for row in rows:
            else:
                print("NOT connected to DB inside parseDarkWebSite")
                xhtml.startElement("p")
                xhtml.characters("NOT connected to DB")
                xhtml.endElement("p")

        except Exception as exc:
            raise exc

        finally:
            xhtml.endDocument()
    # end parseDarkWebSite

    # Parse the TOPIC and populate with its messages
    def parseDarkWebTopic(self, conDB, stream, handler, metadata, context):
        
        xhtml = XHTMLContentHandler(handler, metadata)
        xhtml.startDocument()
        
        # uncomment if used below
        #tmpResources = TemporaryResources()
        try:
            
            extractor = context.get(EmbeddedDocumentExtractor)
            """ tis = TikaInputStream.get(stream, tmpResources)
            tmpFilePath = tis.getFile().getAbsolutePath()
            origFileName = metadata.get(TikaCoreProperties.RESOURCE_NAME_KEY)

            # decode file contents and write the output of your parser to a xhtml document

            conDB = None
            #curDB = None
            
            # get connection to DARKWEB.db
            conDB = sqlite3.connect(tmpFilePath) """
            print("inside parseDarkWebSite")
            if conDB:

                print("connected to DB inside parseDarkWebSite")
                conDB.row_factory = sqlite3.Row

                # prepare DARKWEB.db parsed HTML
                xhtml.startElement("p")
                xhtml.characters("connected to DB")
                xhtml.endElement("p")

                xhtml.startElement("p")
                xhtml.characters("TOPICS:")
                xhtml.endElement("p")
                
                # get the list of topics for the site being processed
                site_GUID = metadata.get("DARKWEB:SiteGUID")
                print("site_GUID = " + str(site_GUID))

                numTopics = 0
                for row in conDB.execute("SELECT TOPIC_GUID,SITES.NAME AS SITE_NAME,PROFILES.NAME as PROFILE_CREATOR_NAME,TITLE,ID,CATEGORIES.CATEGORIES as CATEGORIES FROM TOPICS LEFT JOIN SITES ON TOPICS.SITE_GUID=SITES.SITE_GUID LEFT JOIN PROFILES ON TOPICS.PROFILE_CREATOR_GUID=PROFILES.PROFILE_GUID LEFT JOIN CATEGORIES ON TOPICS.CATEGORIES_ID=CATEGORIES.CATEGORIES_ID WHERE TOPICS.SITE_GUID='" + str(site_GUID) + "'"):

                    numTopics += 1
                    print("numTopics = " + str(numTopics))

                    xhtml.startElement("p")
                    xhtml.characters(row["TITLE"])
                    xhtml.endElement("p")

                    topicMetadata = Metadata()
                    topicMetadata.set(StandardParser.INDEXER_CONTENT_TYPE,self.DARKWEB_TOPIC_MIME)
                    topicMetadata.set(ExtraProperties.ITEM_VIRTUAL_ID,row["TOPIC_GUID"])
                    topicMetadata.set(ExtraProperties.PARENT_VIRTUAL_ID,site_GUID)
                    topicMetadata.set(BasicProps.HASCHILD,"true")
                    topicMetadata.set(TikaCoreProperties.RESOURCE_NAME_KEY,row["TITLE"])
                    topicMetadata.set("DARKWEB:TopicGUID",row["TOPIC_GUID"])
                    topicMetadata.set("DARKWEB:SiteGUID",site_GUID)
                    topicMetadata.set("DARKWEB:SiteName",row["SITE_NAME"])
                    topicMetadata.set("DARKWEB:TopicProfileCreatorName",row["PROFILE_CREATOR_NAME"])
                    topicMetadata.set("DARKWEB:TopicTitle",row["TITLE"])
                    topicMetadata.set("DARKWEB:TopicID",str(row["ID"]))
                    topicMetadata.set("DARKWEB:TopicCategories",row["CATEGORIES"])
                    
                    if (extractor.shouldParseEmbedded(topicMetadata)):
                        extractor.parseEmbedded(EmptyInputStream(),handler,topicMetadata,False)
                # end for row in rows:
            else:
                print("NOT connected to DB inside parseDarkWebSite")
                xhtml.startElement("p")
                xhtml.characters("NOT connected to DB")
                xhtml.endElement("p")

        except Exception as exc:
            raise exc

        finally:
            xhtml.endDocument()
    # end parseDarkWebTopic

    """ def parseSubitem(self, stream, handler, metadata, context):

        mimetype = metadata.get(StandardParser.INDEXER_CONTENT_TYPE)
        print("parser")
        print("mimetype = " + mimetype)
        if mimetype == self.DARKWEB_DB_MIME:
            print("parseDarkWebDB")
            self.parseDarkWebDB(stream, handler, metadata, context)
        elif mimetype == self.DARKWEB_SITE_MIME:
            print("parseDarkWebSite")
            self.parseDarkWebSite(stream, handler, metadata, context)
    # end parse """