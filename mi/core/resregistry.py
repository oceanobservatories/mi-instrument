#!/usr/bin/env python

"""Resource Registry implementation"""

__author__ = 'Michael Meisinger'


from mi.core import bootstrap
from mi.core.bootstrap import IonObject, CFG
from mi.core.exceptions import BadRequest, NotFound, Inconsistent
from mi.core.object import IonObjectBase
from mi.core.registry import getextends

from mi.core.containers import get_ion_ts
from mi.core.log import log

from interface.objects import Attachment, AttachmentType, ResourceModificationType


class ResourceRegistry(object):
    """
    Class that uses a datastore to provide a resource registry.
    The resource registry adds knowledge of resource objects and associations.
    Resources have lifecycle state.
    Add special treatment of Attachment resources
    """

    # -------------------------------------------------------------------------
    # Resource object manipulation

    def create(self, object=None, actor_id=None, object_id=None, attachments=None):
        """
        Accepts object that is to be stored in the data store and tags them with additional data
        (timestamp and such) If actor_id is provided, creates hasOwner association with objects.
        If attachments are provided
        (in dict(att1=dict(data=xyz), att2=dict(data=aaa, content_type='text/plain') form)
        they get attached to the object.
        Returns a tuple containing object and revision identifiers.
        """
        pass

    def create_mult(self, res_list, actor_id=None):
        """Creates a list of resources from objects. Objects may have _id in it to predetermine their ID.
        Returns a list of 2-tuples (resource_id, rev)"""
        pass



    # -------------------------------------------------------------------------
    # Attachment operations

    def create_attachment(self, resource_id='', attachment=None, actor_id=None):
        """
        Creates an Attachment resource from given argument and associates it with the given resource.
        @retval the resource ID for the attachment resource.
        """
       pass

    def read_attachment(self, attachment_id='', include_content=False):
        """
        Returns the metadata of an attachment. Unless indicated otherwise the content returned
        is only a name to the actual attachment content.
        """
        attachment = self.read(attachment_id)
        if not isinstance(attachment, Attachment):
            raise Inconsistent("Object in datastore must be Attachment, not %s" % type(attachment))

        if include_content:
            attachment.content = self.rr_store.read_attachment(attachment_id,
                                                               attachment_name=self.DEFAULT_ATTACHMENT_NAME)
            if attachment.attachment_type == AttachmentType.BLOB:
                if type(attachment.content) is not str:
                    raise BadRequest("Attachment content must be str")

        return attachment

