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
import dateutil.parser
import difflib
import os
import tarfile
import xml.etree.cElementTree as ET

# TODO: logging.

# Configuration
ERROR_DIR = '/tmp/voy_harvest_errs'
TAR_DOWNLOADS = '/home/jstroop/voy_pull'

# Constants
NO_DIFFERENCE = 'NO_DIFFERENCE'
MRX_NS = 'http://www.loc.gov/MARC21/slim'
ET.register_namespace('', MRX_NS)
TAR_EXTENSION = 'tar.gz'

class Record(object):
	'''Internal representation of a MaRCXML record, without Ex Libris' OAI-PMH 
	wrapper MaRCXML record. Contains the first version, the most recent 
	version, and a list of (datetime, diff) two-ples for everything in between.
	'''
	def __init__(self, control_no=None, last_mod=None, diffs=[], 
		first_version=None, current_version=None):
		self.control_no = control_no
		self.last_mod = last_mod
		self.diffs = diffs
		self.first_version = first_version
		self.current_version = current_version

	def __unicode__(self):
		return parseString(self.current_version).toprettyxml(encoding='UTF-8')

	def __str__(self):
		return self.__unicode__()

	def add_version(self, xml_fp):
		oai_etree = ET.parse(xml_fp)

		control_no, new_last_mod = Record.extract_header_vals(xml_fp)

		if self.control_no is None:
		 	self.control_no = control_no 

		new_mrx = Record.extract_marcxml(xml_fp)

		if self.first_version is None:
			self.last_mod = new_last_mod
			self.first_version = new_mrx
			self.current_version = new_mrx
		else:
			diff = self._make_diff(self.current_version, new_mrx, self.last_mod, new_last_mod)
			self.last_mod = new_last_mod
			self.diffs.append((self.last_mod, diff))
			self.current_version = new_mrx

	# Don't love that this and the next meth both have to parse, but we need to 
	# do these things extarnal to the class as well.
	@staticmethod
	def extract_header_vals(exlibris_marcxml_fp):
		'''Return a 2-ple (id, datestamp).
		'''
		try:
			etree = ET.parse(exlibris_marcxml_fp) 
			root = etree.getroot()
			header = root.findall('./ListRecords/record/header')[0]
			control_no = header.findall('identifier')[0].text
			datestamp = dateutil.parser.parse(header.findall('datestamp')[0].text)
		except Exception as e:
			raise HarvesterException(e, exlibris_marcxml_fp)
		else:
			return (control_no, datestamp)

	@staticmethod
	def extract_marcxml(exlibris_marcxml_fp):
		'''Get the MaRCXML as a str.
		'''
		try:
			etree = ET.parse(exlibris_marcxml_fp) 
			root = etree.getroot()
			record_e = root.findall('.//metadata/*')[0]
		except Exception, e:
			raise HarvesterException(e, exlibris_marcxml_fp)
		else:
			return ET.tostring(record_e, encoding='UTF-8').split('?>')[-1].strip()
			# There must be a better way to strip the PI, but this ^ is it for now.

	def _make_diff(self, prev_xml_str, new_xml_str, prev_date, new_date):
		diff = None
		try:
			if prev_xml_str == new_xml_str:
				diff = NO_DIFFERENCE
			else:
				prev_dom = parseString(prev_xml_str)
				pretty_prev = prev_dom.toprettyxml(encoding='UTF-8', newl='\n')
				prev_date_iso = prev_date.isoformat()

				new_dom = parseString(new_xml_str)
				pretty_new = new_dom.toprettyxml(encoding='UTF-8', newl='\n')
				new_date_iso = new_date.isoformat()

				diff_gen = difflib.unified_diff(pretty_prev.split('\n'), 
												pretty_new.split('\n'), 
												fromfiledate=prev_date_iso,
												tofiledate=new_date_iso)
				diff = '\n'.join(diff_gen)
		
		except Exception, e:
			raise e
		finally:
			return diff
		
def unpack_tarball(tarfile_fp):
	'''Unpack a tarball to a temporary directory. Return (str) the path to the 
	directory that was created.

	Raises:
		Exception, in anything else goes wrong.
	'''
	# have to keep files nad dirs sorted by time (name) for later processing
	fname = os.path.basename(tarfile_fp)[:-len(TAR_EXTENSION)-1]
	to_dir = '/tmp/voy_harvest%s' % (fname,)
	os.mkdir(to_dir)
	try:
		t = tarfile.open(tarfile_fp, 'r')
		t.extractall(to_dir)
	except Exception as e:
		if to_dir:
			fps = [os.path.join(to_dir, p) for p in os.listdir(to_dir)]
			map(os.remove, fps)
			os.rmdir(to_dir)
		raise HarvesterException(e, tarfile_fp)
	else:
		os.remove(tarfile_fp)
	finally:
		t.close()
		if to_dir:
			return to_dir

def process_tarball_dir(dp):
	'''Return a list of paths to directories that contain the contents of 
	unpacked tar files.
	'''
	tar_fns = [n for n in filter(lambda n: n.endswith(TAR_EXTENSION), os.listdir(dp))]
	tar_fps = [os.path.join(TAR_DOWNLOADS, fn) for fn in tar_fns]
	unpacked_dps = []
	for tar_fp in tar_fps:
		try:
			dp = unpack_tarball(tar_fp)
		except HarvesterException as he:
			pass # init of HarvesterException handles.
		except Exception as e:
			raise HarvesterException(e, tar_fp)
		else:
			unpacked_dps.append(dp)
	unpacked_dps.sort()
	return unpacked_dps		

def process_file_dir(dp):
	xml_fps = [os.path.join(dp, fn) for fn in os.listdir(dp)]
	xml_fps.sort()
	for xml_fp in xml_fps:
		try:
			control_no, stamp = Record.extract_header_vals(xml_fp)
		except HarvesterException as he:
			pass # init of HarvesterException handles.
		except Exception as e:
			raise HarvesterException(e, xml_fp)
		else:
			pass # print control_no

class HarvesterException(Exception):
	'''Raised when we have trouble with any kind of file: tar or XML. Whether
	it's parsing or missing elements, this class will copy the file at the 
	specified path (fp) to a holding location for further inspection.
	'''
	def __init__(self, original_exception, fp):
		cl_name = original_exception.__class__.__name__
		message = str(original_exception)
		super(HarvesterException, self).__init__(str(original_exception))

		import shutil
		
		if not os.path.exists(ERROR_DIR):
			os.makedirs(ERROR_DIR)

		# account for dupes
		new_path = os.path.join(ERROR_DIR, os.path.basename(fp))
		c = 0
		while os.path.exists(new_path):
			new_path = '%s-%d' % (new_path, c)
			c+=1
		shutil.move(fp, new_path)

		#TODO: log
		bn = os.path.basename(fp)
		os.sys.stderr.write('%s: (%s) %s\n' % (cl_name, fp, message))
		os.sys.stderr.write('Could not handle %s. Moved to %s.\n' % (bn, ERROR_DIR))

		
if __name__ == '__main__':

	dps = process_tarball_dir(TAR_DOWNLOADS)
	for dp in dps:
		process_file_dir(dp)

	# # xml_dp = unpack_tarball('/home/jstroop/workspace/voy_pull/primo.20130228100648.0.tar.gz')
	# r = Record()
	# r.add_version('/tmp/voy_harvest4paRlx/primo.export.130228100002.100180.0.xml')
	# r.add_version('/tmp/altered.xml')

	# from pymongo import Connection
	# connection = Connection('localhost', 27017)
	# db = connection.pulsearch
	# voy_records = db.voy_records
	# voy_records.ensure_index('control_no', unique=True)
	# voy_records.ensure_index('last_mod')

	# # inserted = voy_records.insert(r.__dict__)

	# # print r.diffs[0][1]
	# # print r.control_no, r.last_mod
	# # pp = pprint.PrettyPrinter(indent=4)
	# # pp.pprint(r.__dict__)
	# rec = Record()
	# rec.__dict__ = voy_records.find_one({"control_no": "100180"})
	# print rec.diffs

