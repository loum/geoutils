# pylint: disable=R0903,C0111,R0902
"""The :class:`geoutils.model.Thumb` abstracts an Accumulo metadata
table schema.

"""
__all__ = ["Thumb"]

import tempfile

import geoutils
from geosutils.log import log


class Thumb(geoutils.ModelBase):
    """Thumb Accumulo datastore model.

    """
    _name = 'thumb_library'

    def __init__(self, connection, name=None):
        """:class:`geoutils.model.Thumb models the Accumulo
        ``thumb_library`` table.

        """
        super(Thumb, self).__init__(connection, name)

    def query_thumb(self, key, img_format='JPEG'):
        """Query the metadata component from the datastore.

        **Args:**
            *key*: at this time, *key* relates to the NITF file name
            (less the ``.ntf`` extension) that is used in the current
            schema as the Row ID component of the row key.

            *image_format:* the image reconstruction methods support
            a variety of compression formats including ``JPEG`` (default)
            and ``PNG``

        **Returns:**
            the thumb component of *key*

        """
        log.info('Retrieving thumb for row_id "%s" ...' % key)

        cells = self.query(self.name, key)

        image_fh = tempfile.NamedTemporaryFile('w+b')
        x_coord = y_coord = None
        irep = 'MONO'
        for cell in cells:
            if cell.cf == 'x_coord_size':
                x_coord = cell.cq
            elif cell.cf == 'y_coord_size':
                y_coord = cell.cq
            elif cell.cf == 'irep':
                irep = cell.cq
            elif cell.cf == 'thumb':
                image_fh.write(cell.val)

        log.debug('X|Y: %s|%s' % (x_coord, y_coord))
        dimensions = (int(x_coord), int(y_coord))

        suffix = 'jpg'
        if img_format == 'PNG':
            suffix = 'png'

        temp_obj = tempfile.NamedTemporaryFile(mode='w+b',
                                               suffix=suffix)
        image_fh.seek(0)
        log.debug('Reconstructing image to format "%s"' % img_format)
        if irep == 'MONO':
            image_method = geoutils.GeoImage.reconstruct_image
            image_method(image_fh.read, dimensions).save(temp_obj,
                                                         img_format)
        else:
            dimensions = (int(y_coord), int(x_coord), 3)
            image_method = geoutils.GeoImage.reconstruct_mb_image
            image_method(image_fh.read, dimensions)().save(temp_obj,
                                                            img_format)

        temp_obj.seek(0)

        return temp_obj
