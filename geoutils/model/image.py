# pylint: disable=R0903,C0111,R0902
"""The :class:`geoutils.model.Image` abstracts an Accumulo metadata
table schema.

"""
__all__ = ["Image"]

import tempfile

import geoutils
from oct.utils.log import log


class Image(geoutils.ModelBase):
    """Image Accumulo datastore model.

    """
    _name = 'image_library'

    def __init__(self, name=None):
        """:class:`geoutils.model.Image models the Accumulo
        ``image_library`` table.

        """
        geoutils.ModelBase.__init__(self, name)

    def query_image(self, key, img_format='JPEG'):
        """Query the metadata component from the datastore.

        **Args:**
            *key*: at this time, *key* relates to the NITF file name
            (less the ``.ntf`` extension) that is used in the current
            schema as the Row ID component of the row key.

        **Returns:**
            the image component of *key*

        """
        log.info('Retrieving image for row_id "%s" ...' % key)

        cells = self.query(self.name, key)

        image_fh = tempfile.NamedTemporaryFile('w+b')
        x_coord = y_coord = None
        image = None
        for cell in cells:
            if cell.cf == 'x_coord_size':
                x_coord = cell.cq
            elif cell.cf == 'y_coord_size':
                y_coord = cell.cq
            elif cell.cf == 'image':
                image_fh.write(cell.val)

        log.debug('X|Y: %s|%s' % (x_coord, y_coord))
        log.debug('image: %s' % image)
        dimensions = (int(x_coord), int(y_coord))

        suffix = 'jpg'
        if img_format == 'PNG':
            suffix = 'png'

        temp_obj = tempfile.NamedTemporaryFile(mode='w+b',
                                               suffix=suffix)
        image_fh.seek(0)
        geoutils.GeoImage.reconstruct_image(image_fh.read,
                                            dimensions).save(temp_obj,
                                                             img_format)

        temp_obj.seek(0)

        return temp_obj
