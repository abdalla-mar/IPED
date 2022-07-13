'''
Python parser example.
To use python parsers, first you must install JEP, see https://github.com/sepinf-inc/IPED/wiki/User-Manual#python-modules
Save your parser in 'parsers' folder. The class name must be equal to the script name without the extension. 
For more info about general parser api, see https://github.com/sepinf-inc/IPED/wiki/Contributing
'''

#from hashlib import new
#from typing import List
#from xmlrpc.client import boolean
from org.apache.tika.sax import XHTMLContentHandler
from org.apache.tika.io import TikaInputStream
from org.apache.tika.io import TemporaryResources
from org.apache.tika.metadata import Metadata, Message, TikaCoreProperties
from org.apache.tika.extractor import EmbeddedDocumentExtractor
#from org.apache.tika.mime import MediaType
from iped.properties import BasicProps, ExtraProperties
from iped.utils import EmptyInputStream
from iped.parsers.standard import StandardParser
from iped.parsers.util import ChildPornHashLookup
#from iped.search import IItemSearcher
from iped.data import IItemReader
from iped.datasource import IDataSource
from java.lang import Boolean
from java.io import File
from java.io import FileInputStream
import sqlite3
from os import path
#from datetime import datetime


class DarkWebParser:
    '''
    Example of a python parser. This class must be thread safe.
    One way of achieving this is creating an immutable class i.e. do not create instance attributes.
    '''
    DARKWEB_DB_FOLDERPATH = ""
    DARKWEB_ATTACHMENT_FOLDER_FOUND = False
    DARKWEB = "DARKWEB"
    DARKWEB_SITES_FOLDER = "DARKWEB-SITES"
    DARKWEB_PROFILES_FOLDER = "DARKWEB-PROFILES"
    DARKWEB_SITE_STR = "SITE"
    DARKWEB_PROFILE_STR = "PROFILE"
    DARKWEB_PROFILE_CREATOR_STR = "PROFILE-CREATOR"
    DARKWEB_TOPIC_STR = "TOPIC"
    DARKWEB_MESSAGE_STR = "MESSAGE"
    DARKWEB_ATTACHMENT_STR = "ATTACHMENT"
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
        #return {"application/x-darkweb-db":1}

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
        
        #REMOVE
        print("inside parse")

        xhtml = XHTMLContentHandler(handler, metadata)
        xhtml.startDocument()
        extractor = context.get(EmbeddedDocumentExtractor)
        tmpResources = TemporaryResources()
        conDB = None

        try:

            # get DARKWEB.db folder so attachs can be located
            fileDB = context.get(IItemReader).getDataSource().getSourceFile()
            print("DB path = " + fileDB.getPath())
            self.DARKWEB_DB_FOLDERPATH = path.dirname(fileDB.getPath())
            print("DARKWEB_DB_FOLDERPATH = " + self.DARKWEB_DB_FOLDERPATH)
            # ckeck if attachs folder exists
            if path.isdir(self.DARKWEB_DB_FOLDERPATH + "\\files"):
                self.DARKWEB_ATTACHMENT_FOLDER_FOUND = True
            
            print("DARKWEB_ATTACHMENT_FOLDER_FOUND = " + str(self.DARKWEB_ATTACHMENT_FOLDER_FOUND))
            
            tis = TikaInputStream.get(stream, tmpResources)
            tmpFilePath = tis.getFile().getAbsolutePath()
            #origFileName = metadata.get(TikaCoreProperties.RESOURCE_NAME_KEY)

            # get connection to DARKWEB.db
            conDB = sqlite3.connect(tmpFilePath)

            if conDB:

                # get a row factory so DB columns can be accessed by their names
                conDB.row_factory = sqlite3.Row

                # prepare DARKWEB.db parsed HTML
                #REMOVE
                xhtml.startElement("p")
                xhtml.characters("connected to DB")
                xhtml.endElement("p")
                
                meta = Metadata()
                # create SITES folder under DB
                meta.set(ExtraProperties.EMBEDDED_FOLDER,"true")
                meta.set(ExtraProperties.PARENT_VIRTUAL_ID,str(self.DARKWEB))
                meta.set(ExtraProperties.ITEM_VIRTUAL_ID,str(self.DARKWEB_SITES_FOLDER))
                meta.set(TikaCoreProperties.RESOURCE_NAME_KEY,str(self.DARKWEB_SITES_FOLDER))
                extractor.parseEmbedded(EmptyInputStream(),handler,meta,True)
                # create PROFILES folder under DB
                meta.set(ExtraProperties.EMBEDDED_FOLDER,"true")
                meta.set(ExtraProperties.PARENT_VIRTUAL_ID,str(self.DARKWEB))
                meta.set(ExtraProperties.ITEM_VIRTUAL_ID,str(self.DARKWEB_PROFILES_FOLDER))
                meta.set(TikaCoreProperties.RESOURCE_NAME_KEY,str(self.DARKWEB_PROFILES_FOLDER))
                extractor.parseEmbedded(EmptyInputStream(),handler,meta,True)               

                # parse PROFILES (root)
                self.parseDarkWebProfiles(conDB, stream, handler, metadata, context)
                # parse SITES (root)
                self.parseDarkWebSites(conDB, stream, handler, metadata, context)

                # mark DB as PARSED
                metadata.add("DARKWEB:DBParsed", "true")

            #end if conDB
            else: #NOT connected to DB (conDB is empty)
                #REMOVE
                xhtml.startElement("p")
                xhtml.characters("NOT connected to DB")
                xhtml.endElement("p")

                # mark DB as NOT PARSED
                metadata.add("DARKWEB:DBParsed", "false")

            #end else
        #end try
        except Exception as exc:
            raise exc

        finally:
            xhtml.endDocument()
            tmpResources.close()
            if conDB:
                conDB.close()

        #end finally
    # end parse method

    # Parse the DARKWEB (root) and populate with its PROFILES (root)
    def parseDarkWebProfiles(self, conDB, stream, handler, metadata, context):
        
        try:
            xhtml = XHTMLContentHandler(handler, metadata)
            xhtml.startDocument()

            extractor = context.get(EmbeddedDocumentExtractor)
        
            # uncomment if a temp file is used to process stream
            """ tmpResources = TemporaryResources()
            tis = TikaInputStream.get(stream, tmpResources)
            tmpFilePath = tis.getFile().getAbsolutePath() """
                        
            #REMOVE
            print("inside parseDarkWebProfiles")

            if conDB:

                #REMOVE
                xhtml.startElement("p")
                xhtml.characters("PROFILES:")
                xhtml.endElement("p")

                # get the list of profiles
                numProfile = 0
                for row in conDB.execute("SELECT PROFILE_GUID,PROFILES.NAME AS PROFILE_NAME,PROFILES.SITE_GUID,SITES.NAME AS SITE_NAME FROM PROFILES LEFT JOIN SITES ON PROFILES.SITE_GUID=SITES.SITE_GUID"):
                    numProfile += 1

                    profileMetadata = Metadata()
                    profileMetadata.set(StandardParser.INDEXER_CONTENT_TYPE,str(self.DARKWEB_PROFILE_MIME))
                    #profileMetadata.set(BasicProps.HASCHILD,"true")
                    profileMetadata.set(ExtraProperties.ITEM_VIRTUAL_ID,str(row["PROFILE_GUID"]))
                    profileMetadata.set(ExtraProperties.PARENT_VIRTUAL_ID,str(self.DARKWEB_PROFILES_FOLDER))
                    profileMetadata.set(TikaCoreProperties.RESOURCE_NAME_KEY,str(row["PROFILE_NAME"]) + "@" + str(row["SITE_NAME"]))
                    
                    profileMetadata.set("DARKWEB:ProfileGUID",str(row["PROFILE_GUID"]))
                    profileMetadata.set("DARKWEB:ProfileName",str(row["PROFILE_NAME"]))
                    profileMetadata.set("DARKWEB:SiteGUID",str(row["SITE_GUID"]))
                    profileMetadata.set("DARKWEB:SiteName",str(row["SITE_NAME"]))
                    
                    #REMOVE
                    print("ProfileGUID = " + str(row["PROFILE_GUID"]))
                    print("ProfileName = " + str(row["PROFILE_NAME"]))

                    if (extractor.shouldParseEmbedded(profileMetadata)):
                        print("parsing embedded")
                        extractor.parseEmbedded(EmptyInputStream(),handler,profileMetadata,True)

                # end for (profiles)

                #REMOVE
                print("END FOR PROFILES")
                #REMOVE
                xhtml.startElement("p")
                xhtml.characters("TOTAL PROFILES: " + str(numProfile))
                xhtml.endElement("p")
                
            # end if conDB
            else:
                #REMOVE
                print("NOT connected to DB inside parseDarkWebProfiles")
                xhtml.startElement("p")
                xhtml.characters("NOT connected to DB")
                xhtml.endElement("p")
        # end try
        except Exception as exc:
            raise exc

        finally:
            xhtml.endDocument()
    # end parseDarkWebProfiles

    # Parse the DARKWEB (root) and populate with its SITES (root)
    def parseDarkWebSites(self, conDB, stream, handler, metadata, context):
        
        try:
            xhtml = XHTMLContentHandler(handler, metadata)
            xhtml.startDocument()

            extractor = context.get(EmbeddedDocumentExtractor)
        
            # uncomment if a temp file is used to process stream
            """ tmpResources = TemporaryResources()
            tis = TikaInputStream.get(stream, tmpResources)
            tmpFilePath = tis.getFile().getAbsolutePath() """

            #REMOVE
            print("inside parseDarkWebSites")

            if conDB:
                
                #REMOVE
                xhtml.startElement("p")
                xhtml.characters("SITES:")
                xhtml.endElement("p")

                # get the list of sites
                numSite = 0
                for row in conDB.execute("SELECT * FROM SITES"):
                    numSite += 1

                    siteMetadata = Metadata()
                    siteMetadata.set(StandardParser.INDEXER_CONTENT_TYPE,str(self.DARKWEB_SITE_MIME))
                    siteMetadata.set(BasicProps.HASCHILD,"true")
                    #siteMetadata.set(ExtraProperties.EMBEDDED_FOLDER,"true")
                    siteMetadata.set(ExtraProperties.ITEM_VIRTUAL_ID,str(row["SITE_GUID"]))
                    siteMetadata.set(ExtraProperties.PARENT_VIRTUAL_ID,str(self.DARKWEB_SITES_FOLDER))#self.DARKWEB)
                    siteMetadata.set(TikaCoreProperties.RESOURCE_NAME_KEY,str(row["NAME"]))

                    siteMetadata.set("DARKWEB:SiteGUID",str(row["SITE_GUID"]))
                    siteMetadata.set("DARKWEB:SiteName",str(row["NAME"]))
                    
                    #REMOVE
                    print("site_GUID = " + str(row["SITE_GUID"]))
                    print("site_NAME = " + str(row["NAME"]))

                    if (extractor.shouldParseEmbedded(siteMetadata)):
                        print("parsing embedded")
                        extractor.parseEmbedded(EmptyInputStream(),handler,siteMetadata,True)
                
                    self.parseDarkWebSite(conDB,EmptyInputStream(),handler,siteMetadata,context)

                # end for (sites)

                #REMOVE
                print("END FOR SITES")
                #REMOVE
                xhtml.startElement("p")
                xhtml.characters("TOTAL SITES: " + str(numSite))
                xhtml.endElement("p")

            # end if conDB
            else:
                print("NOT connected to DB inside parseDarkWebSites")
                xhtml.startElement("p")
                xhtml.characters("NOT connected to DB")
                xhtml.endElement("p")
        # end try
        except Exception as exc:
            raise exc

        finally:
            xhtml.endDocument()
    # end parseDarkWebSites

    # Parse the SITE and populate with its TOPICS
    def parseDarkWebSite(self, conDB, stream, handler, metadata, context):
        
        try:
            xhtml = XHTMLContentHandler(handler, metadata)
            xhtml.startDocument()

            extractor = context.get(EmbeddedDocumentExtractor)
        
            # uncomment if a temp file is used to process stream
            """ tmpResources = TemporaryResources()
            tis = TikaInputStream.get(stream, tmpResources)
            tmpFilePath = tis.getFile().getAbsolutePath() """
           
            #REMOVE
            print("inside parseDarkWebSite")

            if conDB:

                #REMOVE
                xhtml.startElement("p")
                xhtml.characters("TOPICS:")
                xhtml.endElement("p")
                
                # get the list of topics for the site being processed
                site_GUID = metadata.get("DARKWEB:SiteGUID")
                
                #REMOVE
                print("site_GUID = " + str(site_GUID))

                numTopic = 0
                for row in conDB.execute("SELECT TOPIC_GUID,TOPICS.SITE_GUID AS SITE_GUID,SITES.NAME AS SITE_NAME,PROFILE_CREATOR_GUID,PROFILES.NAME AS PROFILE_CREATOR_NAME,TITLE,ID,CATEGORIES.CATEGORIES AS CATEGORIES FROM TOPICS LEFT JOIN SITES ON TOPICS.SITE_GUID=SITES.SITE_GUID LEFT JOIN PROFILES ON TOPICS.PROFILE_CREATOR_GUID=PROFILES.PROFILE_GUID LEFT JOIN CATEGORIES ON TOPICS.CATEGORIES_ID=CATEGORIES.CATEGORIES_ID WHERE TOPICS.SITE_GUID='" + str(site_GUID) + "'"):

                    numTopic += 1
                    
                    #REMOVE
                    print("numTopic = " + str(numTopic))

                    topicMetadata = Metadata()
                    topicMetadata.set(StandardParser.INDEXER_CONTENT_TYPE,str(self.DARKWEB_TOPIC_MIME))
                    print('"' + str(row["TOPIC_GUID"]) + '"')
                    topicMetadata.set(ExtraProperties.ITEM_VIRTUAL_ID,str(row["TOPIC_GUID"]))
                    topicMetadata.set(ExtraProperties.PARENT_VIRTUAL_ID,str(site_GUID))
                    topicMetadata.set(BasicProps.HASCHILD,"true")
                    print('"' + str(row["TITLE"]) + '"')
                    topicMetadata.set(TikaCoreProperties.RESOURCE_NAME_KEY,str(row["SITE_NAME"]) + ":topicID=" + str(row["ID"]))
                    
                    topicMetadata.set("DARKWEB:TopicGUID",str(row["TOPIC_GUID"]))
                    topicMetadata.set("DARKWEB:SiteGUID",str(row["SITE_GUID"]))
                    topicMetadata.set("DARKWEB:SiteName",str(row["SITE_NAME"]))
                    topicMetadata.set("DARKWEB:TopicProfileCreatorName",str(row["PROFILE_CREATOR_NAME"]))
                    print('"' + str(row["TITLE"]) + '"')
                    topicMetadata.set("DARKWEB:TopicTitle",str(row["TITLE"]))
                    topicMetadata.set("DARKWEB:TopicID",str(row["ID"]))
                    topicMetadata.set("DARKWEB:TopicCategories",str(row["CATEGORIES"]))
                    
                    if (extractor.shouldParseEmbedded(topicMetadata)):
                        extractor.parseEmbedded(EmptyInputStream(),handler,topicMetadata,True)
               
                    self.parseDarkWebTopic(conDB,EmptyInputStream(),handler,topicMetadata,context)

                # end for (topics)

                #REMOVE
                print("END FOR TOPICS")
                #REMOVE
                xhtml.startElement("p")
                xhtml.characters("TOTAL TOPICS: " + str(numTopic))
                xhtml.endElement("p")

            # end if conDB
            else:
                print("NOT connected to DB inside parseDarkWebSite")
                xhtml.startElement("p")
                xhtml.characters("NOT connected to DB")
                xhtml.endElement("p")
        # edn try
        except Exception as exc:
            raise exc

        finally:
            xhtml.endDocument()
    # end parseDarkWebSite

    # Parse the TOPIC and populate with its messages
    def parseDarkWebTopic(self, conDB, stream, handler, metadata, context):
        
        try:
            
            xhtml = XHTMLContentHandler(handler, metadata)
            xhtml.startDocument()

            extractor = context.get(EmbeddedDocumentExtractor)
        
            # uncomment if a temp file is used to process stream
            """ tmpResources = TemporaryResources()
            tis = TikaInputStream.get(stream, tmpResources)
            tmpFilePath = tis.getFile().getAbsolutePath() """
           
            #REMOVE
            print("inside parseDarkWebTopic")

            if conDB:

                #REMOVE
                xhtml.startElement("p")
                xhtml.characters("MESSAGES:")
                xhtml.endElement("p")
                
                # get the list of messages for the topic being processed
                topic_GUID = metadata.get("DARKWEB:TopicGUID")
                
                #REMOVE
                print("topic_GUID = " + str(topic_GUID))

                numMessage = 0
                for row in conDB.execute("SELECT MESSAGE_GUID,MESSAGES.SITE_GUID AS SITE_GUID,SITES.NAME AS SITE_NAME,MESSAGES.TOPIC_GUID AS TOPIC_GUID,TOPICS.TITLE AS TOPIC_TITLE,TOPICS.ID AS TOPIC_ID,MESSAGES.PROFILE_GUID AS PROFILE_GUID,PROFILES.NAME AS PROFILE_NAME,MESSAGE_URL,DATETIME,TIMEZONE,DATETIME_IS_INFERED,CONTENT,IS_INTEL_ONLY,HAS_ATTACHMENT,HAS_KEYWORDS FROM MESSAGES  LEFT JOIN SITES ON MESSAGES.SITE_GUID=SITES.SITE_GUID LEFT JOIN TOPICS ON MESSAGES.TOPIC_GUID=TOPICS.TOPIC_GUID LEFT JOIN PROFILES ON MESSAGES.PROFILE_GUID=PROFILES.PROFILE_GUID WHERE MESSAGES.TOPIC_GUID='" + str(topic_GUID) + "'"):

                    numMessage += 1

                    #REMOVE
                    print("numMessage = " + str(numMessage))

                    msgMetadata = Metadata()
                    msgMetadata.set(StandardParser.INDEXER_CONTENT_TYPE,str(self.DARKWEB_MESSAGE_MIME))
                    msgMetadata.set(ExtraProperties.ITEM_VIRTUAL_ID,str(row["MESSAGE_GUID"]))
                    msgMetadata.set(ExtraProperties.PARENT_VIRTUAL_ID,str(topic_GUID))
                    #msgMetadata.set(BasicProps.HASCHILD,"true")
                    msgMetadata.set(TikaCoreProperties.RESOURCE_NAME_KEY,str(row["MESSAGE_GUID"]))

                    msgMetadata.add(ExtraProperties.COMMUNICATION_FROM,self.DARKWEB_PROFILE_STR +":"+ str(row["PROFILE_NAME"]))# + "@" + str(row["SITE_NAME"]))
                    msgMetadata.add(ExtraProperties.COMMUNICATION_TO,self.DARKWEB_SITE_STR +":"+ str(row["SITE_NAME"]))#str(row["SITE_GUID"])
                    
                    msgMetadata.set("DARKWEB:MessageGUID",str(row["MESSAGE_GUID"]))
                    msgMetadata.set("DARKWEB:SiteGUID",str(row["SITE_GUID"]))
                    msgMetadata.set("DARKWEB:SiteName",str(row["SITE_NAME"]))
                    msgMetadata.set("DARKWEB:TopicGUID",str(row["TOPIC_GUID"]))
                    msgMetadata.set("DARKWEB:TopicTitle",str(row["TOPIC_TITLE"]))
                    msgMetadata.set("DARKWEB:TopicID",str(row["TOPIC_ID"]))
                    msgMetadata.set("DARKWEB:ProfileGUID",str(row["PROFILE_GUID"]))
                    msgMetadata.set("DARKWEB:ProfileName",str(row["PROFILE_NAME"]))
                    msgMetadata.set("DARKWEB:MessageURL",str(row["MESSAGE_URL"]))
                    datetimeStr = str(row["DATETIME"]) + str(row["TIMEZONE"])
                    msgMetadata.set("DARKWEB:MessageDateTime",str(datetimeStr))#row["DATETIME"])
                    msgMetadata.set(ExtraProperties.MESSAGE_DATE,str(datetimeStr))#row["DATETIME"])
                    msgMetadata.set("DARKWEB:MessageTimezone",str(row["TIMEZONE"]))
                    msgMetadata.set("DARKWEB:MessageDateTimeIsInfered",str.lower(row["DATETIME_IS_INFERED"]))
                    msgMetadata.set("DARKWEB:MessageContent",str(row["CONTENT"]))
                    msgMetadata.set("DARKWEB:MessageIsIntelOnly",str.lower(row["IS_INTEL_ONLY"]))
                    msgMetadata.set("DARKWEB:MessageHasKeyword",str.lower(row["HAS_KEYWORDS"])) #TODO: implement keywords processing
                    msgMetadata.set("DARKWEB:MessageHasAttachment",str.lower(row["HAS_ATTACHMENT"]))
                    
                    #REMOVER
                    print("MessageHasAttachment = " + msgMetadata.get("DARKWEB:MessageHasAttachment"))
                    print(Boolean.parseBoolean(msgMetadata.get("DARKWEB:MessageHasAttachment")))

                    if Boolean.parseBoolean(msgMetadata.get("DARKWEB:MessageHasAttachment")):
                        print("Check MessageHasAttachment")
                        msgMetadata.set(BasicProps.HASCHILD,"true")
                    
                    if (extractor.shouldParseEmbedded(msgMetadata)):
                        extractor.parseEmbedded(EmptyInputStream(),handler,msgMetadata,True)
                    
                    if Boolean.parseBoolean(msgMetadata.get("DARKWEB:MessageHasAttachment")):
                        self.parseDarkWebMessage(conDB,EmptyInputStream(),handler,msgMetadata,context)

                # end for (messages)

                #REMOVE
                print("END FOR MESSAGES")
                #REMOVE
                xhtml.startElement("p")
                xhtml.characters("TOTAL MESSAGES: " + str(numMessage))
                xhtml.endElement("p")

            # end if conDB
            else:                
                print("NOT connected to DB inside parseDarkWebTopic")
                xhtml.startElement("p")
                xhtml.characters("NOT connected to DB")
                xhtml.endElement("p")
        # end try
        except Exception as exc:
            raise exc

        finally:
            xhtml.endDocument()
    # end parseDarkWebTopic


    # Parse the MESSAGE and populate with its attachments
    def parseDarkWebMessage(self, conDB, stream, handler, metadata, context):
        
        try:
            
            xhtml = XHTMLContentHandler(handler, metadata)
            xhtml.startDocument()

            extractor = context.get(EmbeddedDocumentExtractor)
        
            # uncomment if a temp file is used to process stream
            """ tmpResources = TemporaryResources()
            tis = TikaInputStream.get(stream, tmpResources)
            tmpFilePath = tis.getFile().getAbsolutePath() """
           
            #REMOVE
            print("inside parseDarkWebMessage")

            if conDB:

                #REMOVE
                xhtml.startElement("p")
                xhtml.characters("ATTACHMENTS:")
                xhtml.endElement("p")
                
                # get the list of attachments for the message being processed
                msg_GUID = metadata.get("DARKWEB:MessageGUID")
                
                #REMOVE
                print("msg_GUID = " + str(msg_GUID))

                numAttach = 0
                for row in conDB.execute("SELECT FILE_GUID,ATTACHMENTS.SITE_GUID AS SITE_GUID,SITES.NAME AS SITE_NAME,ATTACHMENTS.PROFILE_GUID AS PROFILE_GUID,PROFILES.NAME AS PROFILE_NAME,MESSAGE_GUID,MESSAGE_DATETIME,MESSAGE_TIMEZONE,FILE_URL,FILE_NAME,FILE_SRC_REL_PATH,FILE_SIZE,MIME_TYPE,MD5,SHA1,LOCAL_FILE_HASH,WIDTH,HEIGHT,HAS_EXIF,POTENTIAL_CSAM FROM ATTACHMENTS LEFT JOIN SITES ON SITES.SITE_GUID=ATTACHMENTS.SITE_GUID LEFT JOIN PROFILES ON PROFILES.PROFILE_GUID=ATTACHMENTS.PROFILE_GUID WHERE ATTACHMENTS.MESSAGE_GUID='" + str(msg_GUID) + "'"):

                    numAttach += 1

                    #REMOVE
                    print("numAttach = " + str(numAttach))

                    attachMetadata = Metadata()
                    #attachMetadata.set(StandardParser.INDEXER_CONTENT_TYPE,self.DARKWEB_ATTACHMENT_MIME)
                    attachMetadata.set(ExtraProperties.ITEM_VIRTUAL_ID,str(row["FILE_GUID"]))
                    attachMetadata.set(ExtraProperties.PARENT_VIRTUAL_ID,str(msg_GUID))
                    attachMetadata.set(TikaCoreProperties.RESOURCE_NAME_KEY,str(row["FILE_NAME"]))

                    attachMetadata.add(ExtraProperties.COMMUNICATION_FROM,self.DARKWEB_PROFILE_STR +":"+ str(row["PROFILE_NAME"]))# + "@" + str(row["SITE_NAME"]))
                    attachMetadata.add(ExtraProperties.COMMUNICATION_TO,self.DARKWEB_SITE_STR +":"+ str(row["SITE_NAME"]))#str(row["SITE_GUID"])
                                        
                    attachMetadata.set("DARKWEB:FileIsMessageAttach","true")
                    attachMetadata.set("DARKWEB:FileGUID",str(row["FILE_GUID"]))
                    attachMetadata.set("DARKWEB:SiteGUID",str(row["SITE_GUID"]))
                    attachMetadata.set("DARKWEB:SiteName",str(row["SITE_NAME"]))
                    attachMetadata.set("DARKWEB:ProfileGUID",str(row["PROFILE_GUID"]))
                    attachMetadata.set("DARKWEB:ProfileName",str(row["PROFILE_NAME"]))
                    attachMetadata.set("DARKWEB:FileURL",str(row["FILE_URL"]))
                    attachMetadata.set("DARKWEB:FileName",str(row["FILE_NAME"]))
                    attachMetadata.set("DARKWEB:FileSrcRelPath",str(row["FILE_SRC_REL_PATH"]))
                    attachMetadata.set("DARKWEB:FileSize",str(row["FILE_SIZE"]))
                    attachMetadata.set("DARKWEB:FileMimetype",str(row["MIME_TYPE"]))
                    attachMetadata.set("DARKWEB:FileMD5",str.upper(row["MD5"]))
                    attachMetadata.set("DARKWEB:FileSHA1",str.upper(row["SHA1"]))
                    attachMetadata.set("DARKWEB:FileLocalFileHash",str.upper(row["LOCAL_FILE_HASH"]))
                    attachMetadata.set("DARKWEB:FileWidth",str(row["WIDTH"]))
                    attachMetadata.set("DARKWEB:FileHeight",str(row["HEIGHT"]))
                    attachMetadata.set("DARKWEB:FileHasEXIF",str.lower(row["HAS_EXIF"]))
                    attachMetadata.set("DARKWEB:FileIsPotencialCSAM",str.lower(row["POTENTIAL_CSAM"]))

                    attachMetadata.set("DARKWEB:MessageGUID",str(row["MESSAGE_GUID"]))
                    attachMetadata.set("DARKWEB:TopicGUID",str(metadata.get("DARKWEB:TopicGUID")))
                    attachMetadata.set("DARKWEB:TopicTitle",str(metadata.get("DARKWEB:TopicTitle")))
                    attachMetadata.set("DARKWEB:TopicID",str(metadata.get("DARKWEB:TopicID")))
                    attachMetadata.set("DARKWEB:MessageURL",str(metadata.get("DARKWEB:MessageURL")))
                    attachMetadata.set("DARKWEB:FileSharedDateTimeIsInfered",str.lower(metadata.get("DARKWEB:MessageDateTimeIsInfered")))
                    attachMetadata.set("DARKWEB:MessageDateTimeIsInfered",str.lower(metadata.get("DARKWEB:MessageDateTimeIsInfered")))
                    attachMetadata.set("DARKWEB:MessageContent",str(metadata.get("DARKWEB:MessageContent")))
                    attachMetadata.set("DARKWEB:MessageIsIntelOnly",str.lower(metadata.get("DARKWEB:MessageIsIntelOnly")))

                    #initialize timestamps metadata with the values from the attachment row
                    datetimeStr = str(row["MESSAGE_DATETIME"]) + str(row["MESSAGE_TIMEZONE"])
                    attachMetadata.set("DARKWEB:FileSharedDateTime",str(datetimeStr))
                    attachMetadata.set("DARKWEB:FileSharedTimezone",str(row["MESSAGE_TIMEZONE"]))
                    attachMetadata.set("DARKWEB:MessageDateTime",str(datetimeStr))
                    attachMetadata.set("DARKWEB:MessageTimezone",str(row["MESSAGE_TIMEZONE"]))
                    attachMetadata.set(ExtraProperties.MESSAGE_DATE,str(datetimeStr))

                    #check if the parent message has valid timestamps and prefer that one
                    msgTimestamp = metadata.get("DARKWEB:MessageDateTime")
                    if msgTimestamp:
                        attachMetadata.set("DARKWEB:FileSharedDateTime",str(msgTimestamp))
                        attachMetadata.set("DARKWEB:FileSharedTimezone",str(metadata.get("DARKWEB:MessageTimezone")))
                        attachMetadata.set("DARKWEB:MessageDateTime",str(msgTimestamp))
                        attachMetadata.set("DARKWEB:MessageTimezone",str(metadata.get("DARKWEB:MessageTimezone")))
                        attachMetadata.set(ExtraProperties.MESSAGE_DATE,str(msgTimestamp))
                    
                    #Check attachment against CPHashSets
                    hashSets = ChildPornHashLookup.lookupHash(str.upper(row["SHA1"]))
                    if not hashSets.isEmpty():
                        attachMetadata.set("hash:status", "pedo")
                        for set in hashSets:
                            attachMetadata.add("hash:set", set)
                    
                    #Search attachment and create inputfilestream
                    attachPath = str(self.DARKWEB_DB_FOLDERPATH) + "\\" + str(row["FILE_SRC_REL_PATH"])
                    print('ATTACH path = ' + attachPath)
                    if path.exists(attachPath):
                        print("FILE FOUND!!!")
                        try:
                            fileInputStream = FileInputStream(attachPath)
                            extractor.parseEmbedded(fileInputStream,handler,attachMetadata,False)
                        except Exception as exc:
                            raise exc
                        finally:
                            fileInputStream.close()
                    else:
                        print("FILE NOT FOUND!!!")
                        if (extractor.shouldParseEmbedded(attachMetadata)):
                            extractor.parseEmbedded(EmptyInputStream(),handler,attachMetadata,True)

                # end for (attachments)

                #REMOVE
                print("END FOR ATTACHMENTS")
                #REMOVE
                xhtml.startElement("p")
                xhtml.characters("TOTAL ATTACHMENTS: " + str(numAttach))
                xhtml.endElement("p")

            # end if conDB
            else:                
                print("NOT connected to DB inside parseDarkWebMessage")
                xhtml.startElement("p")
                xhtml.characters("NOT connected to DB")
                xhtml.endElement("p")
        # end try
        except Exception as exc:
            raise exc

        finally:
            xhtml.endDocument()
    # end parseDarkWebMessage

    #self.addComLinks(,ExtraProperties.COMMUNICATION_FROM,self.DARKWEB_PROFILE_STR,"PROFILE_GUID","SELECT PROFILE_GUID,NAME FROM PROFILES WHERE SITE_GUID='" + str(row["SITE_GUID"] + "'")
    def addComLinks(self, conDB, metadata, comProp, prefix, column, select):
        
        if not conDB:
            return
        
        for row in conDB.execute(select):
            metadata.add(str(comProp),str(prefix) + ":" + str(row[column]))

    #end addComLinks
    
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