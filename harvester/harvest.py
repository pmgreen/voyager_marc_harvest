#! /usr/bin/env python
#-*-coding:utf-8-*-
'''
harvest.py
==========

Responsible for getting records from Voyager publishing service, parsing them 
into real MaRCXML, and loading them into the database.

Author: <a href="jstroop@princeton.edu">Jon Stroop</a>
Since: 2013-03-01
'''
#
# Flow
# ----
# 1. Build a string that matches the Voyger publishing naming conventions 
#	 (awesome!)
# 2. Connect to SFTP server
# 3. SCP tar.gz files to local machine
# 4. Disconnect from SFTP server
# 5. Unpack each tar.gz file
# 6. For each XML file in each tar.gz file, parse out the XML and metadata; 
#    either into an object TBD (probably) or directly to the database.
# 7. Load/update the object in the database. 
#
# Naming Convention Note
# ----------------------
# 'fp' and 'fps' are used in variable names throughout (e.g. 'tarfile_fp', 
# 'xml_fps') to stand for 'file path' and 'file paths' and clearly distinguish 
# between file pointers and actual file objects. 'dp' and 'dps' are used 
# similarly for directories.
#

from xml.dom.minidom import parseString
import difflib
import os
import tarfile
import tempfile
import xml.etree.cElementTree as ET

# TODO: logging.

TMP_PREFIX = 'voy_harvest'
NO_DIFFERENCE = 'NO_DIFFERENCE'
MRX_NS = 'http://www.loc.gov/MARC21/slim'
ET.register_namespace('', MRX_NS)

class Record(object):
	'''Internal representation of an Ex Libris OAI-PMH wrapped MaRCXML record.
	This contains the first version, the most recent version, and a list of
	(datestamp, diff) two-ples for everything in between.
	'''
	# TODO __slots__ once the dust has settled.
	def __init__(self, control_no=None, last_mod=None, diffs=[], 
		first_version=None, current_version=None):
		self.control_no = control_no
		self.last_mod = last_mod
		self.diffs = diffs
		self.first_version = first_version
		self.current_version = current_version

	def __unicode__(self):
		return parseString(self.current_version).toprettyxml()

	def __str__(self):
		return self.__unicode__()

	def add_version(self, xml_fp):
		oai_etree = ET.parse(xml_fp)

		control_no, new_last_mod = self._extract_header_vals(oai_etree)

		if self.control_no is None:
		 	self.control_no = control_no 

		new_mrx = self._extract_marcxml(oai_etree)

		if self.first_version is None:
			self.last_mod = new_last_mod
			self.first_version = new_mrx
			self.current_version = new_mrx
		else:
			diff = self._make_diff(self.current_version, new_mrx, self.last_mod, new_last_mod)
			self.last_mod = new_last_mod
			self.diffs.append((self.last_mod, diff))
			self.current_version = new_mrx

	def _extract_header_vals(self, oai_etree):
		'''Return a 2-ple (id, datestamp)'''
		root = oai_etree.getroot()
		# may have to modify if EL ever gets ListRecords into the right NS
		header = root.findall('./ListRecords/record/header')[0]
		control_no = header.findall('identifier')[0].text
		datestamp = header.findall('datestamp')[0].text #todo: make a datetime
		return (control_no, datestamp)

	def _extract_marcxml(self, oai_etree):
		'''Get the MaRCXML as a str'''
		root = oai_etree.getroot()
		record_e = root.findall('.//metadata/*')[0]
		return ET.tostring(record_e, encoding='UTF-8')

	def _make_diff(self, prev_xml_str, new_xml_str, prev_date, new_date):
		diff = None
		try:
			if prev_xml_str == new_xml_str:
				diff = NO_DIFFERENCE
			else:
				prev_dom = parseString(prev_xml_str)
				pretty_prev = prev_dom.toprettyxml(encoding='UTF-8', newl='\n')

				new_dom = parseString(new_xml_str)
				pretty_new = new_dom.toprettyxml(encoding='UTF-8', newl='\n')

				diff_gen = difflib.unified_diff(pretty_prev.split('\n'), 
												pretty_new.split('\n'), 
												fromfiledate=prev_date, 
												tofiledate=new_date)
				diff = '\n'.join(diff_gen)
		
		except Exception, e:
			raise e
		finally:
			return diff
		


def unpack_tarball(tarfile_fp):
	'''Unpack a tarball to a temporary directory.
	
	Args:
		tarfile_fp (str): Path to the tarball.
		to_dir (str): Where to unpack.

	Returns:
		(str) The path to the directory that was created.

	Raises:
		OSError, if the temporary directory can't be created.
		Exception, in anything else goes wrong.
	'''
	to_dir = tempfile.mkdtemp(prefix=TMP_PREFIX)
	try:
		t = tarfile.open(tarfile_fp, 'r')
		t.extractall(to_dir)
		
	except OSError as ose:
		raise ose
	except Exception as e:
		if to_dir:
			fps = [os.path.join(to_dir, p) for p in os.listdir(to_dir)]
			map(os.remove, fps)
			os.rmdir(to_dir)
		raise e
	finally:
		t.close()
		if to_dir:
			return to_dir

def process_tmp_dir(dp):
	xml_fps = [os.path.join(dp, fn) for fn in os.listdir(dp)]
	# TODO

if __name__ == '__main__':
	import sys
	# try to unpack all tar files
	try:
		pass
	# If something goes wrong, log the problem and move the tar file to a 
	# holding space for later examination.
	except Exception, e:
		# TODO
		raise
	finally:
		pass

	# xml_dp = unpack_tarball('/home/jstroop/workspace/voy_pull/primo.20130228100648.0.tar.gz')
	r = Record()

	r.add_version('/tmp/voy_harvest4paRlx/primo.export.130228100002.100180.0.xml')
	# r.add_version('/tmp/voy_harvest4paRlx/primo.export.130228100002.100180.0.xml')
	r.add_version('/tmp/altered.xml')
	print r.diffs[0][1]
	# print r.control_no, r.last_mod
