#! /usr/bin/env python
#-*-coding:utf-8-*-
'''
harvest.py
==========
Getting records from Voyager publishing service, parse them into real MaRCXML 
and save to a directory as MaRC collections for further processing.

Errors go to stderr, info to stdout.

Author: <a href="jstroop@princeton.edu">Jon Stroop</a>
Since: 2013-03-01
'''
#
# Naming Convention Note
# ----------------------
# 'fp' and 'fps' are used in variable names throughout (e.g. 'tarfile_fp', 
# 'xml_fps') to stand for 'file path' and 'file paths' and clearly distinguish 
# between file pointers and actual file objects. 'dp' and 'dps' are used 
# similarly for directories.
#

from xml.dom.minidom import parseString
import ConfigParser
import dateutil.parser
import difflib
import logging
import os
import paramiko
import sys
import tarfile
import xml.etree.cElementTree as ET

class StdErrFilter(logging.Filter):
	def filter(self,record):
		return 1 if record.levelno >= 30 else 0

class StdOutFilter(logging.Filter):
	def filter(self,record):
		# To show DEBUG
		# return 1 if record.levelno <= 20 else 0
		return 1 if record.levelno <= 20 and record.levelno > 1

# Configuration / Constants
config = ConfigParser.ConfigParser()
conf_fp = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'conf.ini')
config.read(os.path.join(conf_fp))

COLLECTION_END = '</collection>'
COLLECTION_START = '<collection xmlns="http://www.loc.gov/MARC21/slim">'
CREATE_UPDATE = 'CREATE_UPDATE'
DELETED='deleted'
DELETE_IDS_FP = config.get('harvester', 'deletes_file')
ERROR_DIR = config.get('harvester', 'error_dir')
FINAL_XML_DIR = config.get('harvester', 'final_xml_dir')
LOG_FMT = '%(asctime)s (%(name)s) [%(levelname)s]: %(message)s'
MRX_NS = 'http://www.loc.gov/MARC21/slim'
NO_DIFFERENCE = 'NO_DIFFERENCE'
SSH_CHANGED_SINCE_FILE = config.get('ssh', 'changed_since_file')
SSH_FIND_UTIL = config.get('ssh', 'find_util')
SSH_PW = config.get('ssh', 'pw')
SSH_SERVER = config.get('ssh', 'server')
SSH_SERVER_DIR = config.get('ssh', 'server_dir')
SSH_USER = config.get('ssh', 'user')
TAR_DOWNLOADS = config.get('harvester', 'tar_downloads')
TAR_EXTENSION = 'tar.gz'
TMP_UNPACK = config.get('harvester', 'tmp_unpack')

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter(fmt=LOG_FMT)

err_handler = logging.StreamHandler(sys.__stderr__)
err_handler.addFilter(StdErrFilter())
err_handler.setFormatter(formatter)
logger.addHandler(err_handler)

out_handler = logging.StreamHandler(sys.__stdout__)
out_handler.addFilter(StdOutFilter())
out_handler.setFormatter(formatter)
logger.addHandler(out_handler)

# Make dirs
__dps = (ERROR_DIR, TAR_DOWNLOADS, TMP_UNPACK, FINAL_XML_DIR)
[os.makedirs(d) for d in __dps if not os.path.exists(d)]

ET.register_namespace('', MRX_NS)

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

	# Don't love that this and the next meth both have to parse, but we need to 
	# do these things external to the class as well.
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

	@staticmethod
	def extract_control_status_mrx(exlibris_marcxml_fp):
		'''Get a (control number, status, mrx) three-tuple
		'''
		try:
			etree = ET.parse(exlibris_marcxml_fp) 
			root = etree.getroot()
			header = root.find('./ListRecords/record/header')

			control = header.find('identifier').text

			status_attr = header.attrib.get('status')
			status = status_attr if status_attr else CREATE_UPDATE

			record_e = root.find('.//metadata/*')

		except Exception, e:
			raise HarvesterException(e, exlibris_marcxml_fp)
		else:
			mrx = ET.tostring(record_e, encoding='UTF-8').split('?>')[-1].strip()
			mrx = mrx.replace(' xmlns="%s"' % MRX_NS, '')
			return (control, status, mrx)

class HarvesterException(Exception):
	'''Raised when we have trouble with any (either) kind of file: tar or XML. 
	Whether	it's parsing or missing elements, at init this class will copy the 
	file at the specified path (fp) to a holding location for further (human) 
	inspection.
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

		logger.warn('%s: (%s) %s' % (cl_name, fp, message))
		logger.warn('Could not handle %s. Moved to %s.' % (fp, new_path))

##############
# Functions
##############

def unpack_tarball(tarfile_fp):
	'''Unpack a tarball to a temporary directory. Return (str) the path to the 
	directory that was created.
	'''
	# have to keep files nad dirs sorted by time (name) for later processing
	fname = os.path.basename(tarfile_fp)[:-len(TAR_EXTENSION)-1]
	to_dir = os.path.join(TMP_UNPACK, fname)
	os.mkdir(to_dir)
	try:
		t = tarfile.open(tarfile_fp, 'r')
		t.extractall(to_dir)
	except Exception as e:
		if to_dir:
			fps = [os.path.join(to_dir, p) for p in os.listdir(to_dir)]
			map(os.remove, fps)
			os.rmdir(to_dir)
			logger.warn('Removed %s as a result of follow Exception' % to_dir)
		raise HarvesterException(e, tarfile_fp)
	else:
		logger.debug('Unpacked %s to %s' % (tarfile_fp, to_dir))
		os.remove(tarfile_fp)
	finally:
		t.close()
		if to_dir:
			return to_dir

def process_tarball_dir():
	'''Return a list of paths to directories that contain the contents of 
	unpacked tar files.
	'''
	__filter = lambda n: n.endswith(TAR_EXTENSION)
	tar_fns = [n for n in filter(__filter, os.listdir(TAR_DOWNLOADS))]
	tar_fps = [os.path.join(TAR_DOWNLOADS, fn) for fn in tar_fns]
	unpacked_dps = []
	for tar_fp in tar_fps:
		try:
			unpacked_dp = unpack_tarball(tar_fp)
		except HarvesterException as he:
			pass # init of HarvesterException handles.
		except Exception as e:
			raise HarvesterException(e, tar_fp)
			continue
		else:
			unpacked_dps.append(unpacked_dp)
	unpacked_dps.sort()
	return unpacked_dps		

def process_file_dir(dp):
	'''Go through a dir of XML files and get the MaRCXML.
	'''
	xml_fps = [os.path.join(dp, fn) for fn in os.listdir(dp)]
	xml_fps.sort()

	if len(xml_fps) > 0:
		bn = os.path.basename(dp) + '.mrx'
		out_fp = os.path.join(FINAL_XML_DIR, bn)

		with open(out_fp, 'wb') as f:
			f.write(COLLECTION_START)
			for xml_fp in xml_fps:
				try:
					# control_no, stamp = Record.extract_header_vals(xml_fp)
					control, status, mrx = Record.extract_control_status_mrx(xml_fp)
				except HarvesterException as he:
					pass # init of HarvesterException handles.
				except Exception as e:
					raise HarvesterException(e, xml_fp)
					continue
				else:
					if status == DELETED: 
						logger.info('%s: %s' % (control, status))
						with open(DELETE_IDS_FP, 'ab') as df:
							df.write(control + os.linesep)
					else:
						f.write(mrx)
					logger.debug('Wrote %s to %s' % (control, out_fp))
					os.remove(xml_fp)

			f.write(COLLECTION_END)

	os.rmdir(dp)

def harvest():
	try:
		ssh_client = paramiko.SSHClient()
		ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		ssh_client.connect(SSH_SERVER, username=SSH_USER, password=SSH_PW)
		cmd = '%s %s -newer %s' % (SSH_FIND_UTIL, SSH_SERVER_DIR, SSH_CHANGED_SINCE_FILE)
		stdin, stdout, stderr = ssh_client.exec_command(cmd)
		remote_paths = map(str.strip, stdout.readlines())
		remote_paths.remove(SSH_SERVER_DIR)
		sftp_client = ssh_client.open_sftp()
		for remote_path in remote_paths:
			bn = os.path.basename(remote_path)
			local_path = os.path.join(TAR_DOWNLOADS, bn)
			logger.debug('Getting %s' % remote_path)
			try:
				sftp_client.get(remote_path, local_path)
			except Exception, e:
				logger.critical('%s: %s' % (e.__class__.__name__, str(e)))
				continue
	except Exception, e:
		logger.critical('%s: %s' % (e.__class__.__name__, str(e)))
	finally:
		if ssh_client:
			ssh_client.close()
		if sftp_client:
			sftp_client.close()
	return True

if __name__ == '__main__':
	if harvest(): 
		map(process_file_dir, process_tarball_dir())

## Longhand:

# harvest_ok = harvest()
# if harvest_ok:
# 	dps = process_tarball_dir()
# 	for dp in dps:
# 	 	process_file_dir(dp)


