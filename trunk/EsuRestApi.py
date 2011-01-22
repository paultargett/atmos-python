#!/usr/bin/env python
import hmac, base64, hashlib, time
import urllib2, urllib
import re, urlparse
#from eventlet.green import urllib2
#import eventlet

class EsuRestApi(object):
 
    ID_EXTRACTOR = "/[0-9a-zA-Z]+/objects/([0-9a-f]{44})"
 
    def __init__(self, host, port, uid, secret):
        self.host, self.port, self.uid, self.secret = host, port, uid, secret
         
        if self.port == 443:
            self.scheme, self.netloc, self.path, self.params, self.query, self.fragment = "https", host, '', '', '', ''
            self.urlparts = (self.scheme, self.netloc, self.path, self.params, self.query, self.fragment)
            self.url = urlparse.urlunparse(self.urlparts)
        else:
            self.scheme, self.netloc, self.path, self.params, self.query, self.fragment = "http", host, '', '', '', ''
            self.urlparts = (self.scheme, self.netloc, self.path, self.params, self.query, self.fragment)
            self.url = urlparse.urlunparse(self.urlparts)
 
  
    # Creates an object using the Atmos object interface
    def create_object(self, listable_meta = None, non_listable_meta = None, mime_type = None, data = None):
    
        if listable_meta:
            meta_string = self.__process_metadata(listable_meta)
            
        if non_listable_meta:
            nl_meta_string = self.__process_metadata(non_listable_meta)
            
        if mime_type == None and data != None:
            mime_type = "application/octet-stream"
         
        now = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
    
        headers = "POST\n"
     
        if data:
            headers += mime_type+"\n"
     
        headers += "\n"
        headers += now+"\n"
        headers += "/rest/objects\n"
        headers += "x-emc-date:"+now+"\n"
     
        headers += "x-emc-uid:"+self.uid
        request = urllib2.Request(self.url+"/rest/objects")
     
        if data:
            request.add_header("content-type", mime_type)
     
        request.add_header("date", now)
        request.add_header("host", self.host)
        request.add_header("x-emc-date", now)
        
        if listable_meta:
            headers += "x-emc-listable-meta:"+meta_string+"\n"
            request.add_header("x-emc-listable-meta", meta_string)
            
        if non_listable_meta:
            headers += "x-emc-meta:"+nl_meta_string+"\n"
            request.add_header("x-emc-meta", nl_meta_string)
     
        request.add_header("x-emc-uid", self.uid)
        request.add_data(data)
    
        hashout = self.__sign(headers)
     
        object_id = self.__send_request(request, hashout, headers)

        return object_id
 
    
    def create_object_on_path(self, path, listable_meta = None, non_listable_meta = None, mime_type = None, data = None):
     
        if mime_type == None and data != None:
            mime_type = "application/octet-stream"
         
        now = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
    
        headers = "POST\n"
     
        if data:
            headers += mime_type+"\n"
     
        headers += "\n"
        headers += now+"\n"
        headers += "/rest/namespace"+path+"\n"
        headers += "x-emc-date:"+now+"\n"
     
        request = urllib2.Request(self.url+"/rest/namespace"+path)

        if listable_meta:
            meta_string = self.__process_metadata(listable_meta)
            headers += "x-emc-listable-meta:"+meta_string+"\n"
            request.add_header("x-emc-listable-meta", meta_string)
            
        if non_listable_meta:
            nl_meta_string = self.__process_metadata(non_listable_meta)
            headers += "x-emc-meta:"+nl_meta_string+"\n"
            request.add_header("x-emc-meta", nl_meta_string)

        headers += "x-emc-uid:"+self.uid
         
        if data:
            request.add_header("content-type", mime_type)
     
        request.add_header("date", now)
        request.add_header("host", self.host)
        request.add_header("x-emc-date", now)
          
        request.add_header("x-emc-uid", self.uid)
        request.add_data(data)
         
        hashout = self.__sign(headers)

        try:
            response = self.__send_request(request, hashout, headers)
      
        except urllib2.HTTPError as e:
            error_message = e.read()
            print error_message
         
        else:                                                                   # If there was no HTTPError, parse the location header in the response body to get the object_id
            location = response.info().getheader('location')
            search = re.search(self.ID_EXTRACTOR, location)
            reg = search.groups() 
            object_id = reg[0]
            return object_id
  
    def list_directory(self, path):
      
        mime_type = "application/octet-stream"
         
        now = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
    
        headers = "GET\n"
     
        headers += mime_type+"\n"
     
        headers += "\n"
        headers += now+"\n"
        headers += "/rest/namespace"+path+"\n"
        headers += "x-emc-date:"+now+"\n"
        headers += "x-emc-uid:"+self.uid
    
        request = urllib2.Request(self.url+"/rest/namespace"+path)
        request.add_header("content-type", mime_type)
        request.add_header("date", now)
        request.add_header("host", self.host)
        request.add_header("x-emc-date", now)
        request.add_header("x-emc-uid", self.uid)

        hashout = self.__sign(headers)
      
        try:
            response = self.__send_request(request, hashout, headers)
      
        except urllib2.HTTPError as e:
            error_message = e.read()
            print error_message
         
        else:
            dir_list = response.read()
            return dir_list
      
    def delete_object(self, object_id):
      
        mime_type = "application/octet-stream"
        now = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
    
        headers = "DELETE\n"
        headers += mime_type+"\n"
        headers += "\n"
        headers += now+"\n"
        headers += "/rest/objects/"+object_id+"\n"
        headers += "x-emc-date:"+now+"\n"
        headers += "x-emc-uid:"+self.uid
    
        request = RequestWithMethod("DELETE", "%s/%s" % (self.url+"/rest/objects", object_id))
        request.add_header("content-type", mime_type)
        request.add_header("date", now)
        request.add_header("host", self.host)
        request.add_header("x-emc-date", now)
        request.add_header("x-emc-uid", self.uid)
    
        hashout = self.__sign(headers)

        try:
            response = self.__send_request(request, hashout, headers)

        except urllib2.HTTPError as e:
            error_message = e.read()
            print error_message
         
        else:                                                                                               # If there was no HTTPError, parse the location header in the response body to get the object_id
            return response

      
    def read_object(self, object_id):
        mime_type = "application/octet-stream"
        now = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
    
        headers = "GET\n"
        headers += mime_type+"\n"
        headers += "\n"
        headers += now+"\n"
        headers += "/rest/objects/"+object_id+"\n"
        headers += "x-emc-date:"+now+"\n"
        headers += "x-emc-uid:"+self.uid
    
        request = urllib2.Request(self.url+"/rest/objects/"+object_id)
        request.add_header("content-type", mime_type)
        request.add_header("date", now)
        request.add_header("host", self.host)
        request.add_header("x-emc-date", now)
        request.add_header("x-emc-uid", self.uid)

        hashout = self.__sign(headers)
      
        try:
            response = self.__send_request(request, hashout, headers)
      
        except urllib2.HTTPError as e:
            error_message = e.read()
            print error_message
         
        else:
            body = response.read()
            return body
        
    def get_shareable_url(self, object_id, expiration):
        uid_dict = {}
        uid_dict["uid"] = self.uid
        encoded_uid = urllib.urlencode(uid_dict)
            
        sb = "GET\n"
        sb += "/rest/objects/"+object_id+"\n"
        sb += self.uid+"\n"
        sb += str(expiration)
               
        signature = self.__sign(sb)
        sig_dict = {}
        sig_dict["signature"] = signature
        encoded_sig = urllib.urlencode(sig_dict)
        resource = "/rest/objects/"+object_id+ "?" + encoded_uid + "&expires=" + str(expiration) + "&" + encoded_sig
        url = self.scheme + "://" + self.host + resource
        
        return url
    
    
    def create_directory(self, dir_path):
      
        now = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
    
        headers = "POST\n"
        headers += "\n"
        headers += "\n"
        headers += now+"\n"
        headers += "/rest/namespace/"+dir_path+"\n"
        headers += "x-emc-date:"+now+"\n"
        headers += "x-emc-uid:"+self.uid
    
        request = RequestWithMethod("POST", "%s/%s" % (self.url+"/rest/namespace", dir_path))
        request.add_header("date", now)
        request.add_header("host", self.host)
        request.add_header("x-emc-date", now)
        request.add_header("x-emc-uid", self.uid)
    
        hashout = self.__sign(headers)

        try:
            response = self.__send_request(request, hashout, headers)

        except urllib2.HTTPError as e:
            error_message = e.read()
            print error_message
         
        else:                                                                                               # If there was no HTTPError, parse the location header in the response body to get the object_id
            return response
    
    # Renames won't work before Atmos 1.3.x -- Need to test this on those versions.
    def rename_object(self, source, destination, force):
      
        now = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
    
        headers = "POST\n"
        headers += "\n"
        headers += "\n"
        headers += now+"\n"
        headers += "/rest/namespace/"+source+"\n"
        headers += "x-emc-date:"+now+"\n"
        headers += "x-emc-path:"+destination+"\n"
        headers += "x-emc-uid:"+self.uid
    
        request = RequestWithMethod("POST", "%s/%s" % (self.url+"/rest/namespace", source))
        request.add_header("date", now)
        request.add_header("host", self.host)
        request.add_header("x-emc-date", now)
        request.add_header("x-emc-path", destination)
        request.add_header("x-emc-uid", self.uid)
    
        hashout = self.__sign(headers)

        try:
            response = self.__send_request(request, hashout, headers)

        except urllib2.HTTPError as e:
            error_message = e.read()
            print error_message
         
        else:                                                                                               # If there was no HTTPError, parse the location header in the response body to get the object_id
            return response

                   
    #Actually send the request
    def __send_request(self, request, hashout, headers):
        headers += ("\nx-emc-signature:"+hashout)
        print headers+"\n"
        request.add_header("x-emc-signature", hashout)

        response = urllib2.urlopen(request)
      
        return response
    
    # Private method used to sign HTTP requests    
    def __sign(self, headers):
        decodedkey = base64.b64decode(self.secret)                                                          
        hash = hmac.new(decodedkey, headers, hashlib.sha1).digest()                                         
        hashout = base64.encodestring(hash).strip()                                                         
   
        return hashout
    
    def __process_metadata(self, metadata):
        meta_string = ""
        for k,v in metadata.iteritems():
            meta_string += "%s=%s," % (k,v)
        meta_string = meta_string[0:-1]                                                                 # Create a new string using a slice to remove the trailing comma                                                       
        meta_string = ' '.join(meta_string.split())                                                     # Remove two or more spaces if they exist
        
        return meta_string


class RequestWithMethod(urllib2.Request):                                                                   # Subclass the urllib2.Request object and then override the HTTP methom

    def __init__(self, method, *args, **kwargs):
        self._method = method
        urllib2.Request.__init__(self, *args, **kwargs)
   
    def get_method(self):
        return self._method


#TODO:  There's a lot that could be added so that this wrapper is in parity with the other wrappers
#       We're also not doing range updates so large objects will be a problem at the moment.
   
    
    def list_objects():
        pass
    
    def list_objects_with_metadata():
        pass
    
    def get_listable_tags():
        pass
    
    def list_user_metadata():
        pass
    
    def set_user_metadata():
        pass
    
    def update_object():
        pass
    
    def get_user_metadata():
        pass
    
    def get_acl():
        pass
    
    def set_acl():
        pass 
  
    def delete_user_metadata():
        pass
    
    def list_versions():
        pass
    
    def version_object():
        pass
    
    def restore_version():
        pass
    
    def get_system_metadata():
        pass
    
    def get_service_information():
        pass
 
#
#
#def use_async_io():
#    for item in i:
#        now = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
#        for item in pool.imap(__get_headers(now)):
#            print i
#
#pool = eventlet.GreenPool()








