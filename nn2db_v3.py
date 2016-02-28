# -*- coding: utf-8 -*-
#
# Скрипт для разбора xls файла XLS_BOOK содержашего информацию об 
# отделениях и заполнения этими данными базы SQLite (в папке DB).
# База в свою очередь будет использоваться в Django.
#
# Задача осложняется тем, что весь адрес расположен в одной ячейке.
# 
# С целью соблюдения конфедициальности вместо настоящего файла с данными
# использован тестовый файлик.
#
# Для удобства контроля данные выводятся в виде SQL и TXT в папку LOG
#
# В данном скрипте я постарался всё красиво расписать по классам.
# 
#
#


from __future__ import unicode_literals
from functools import wraps

import codecs 
import xlrd
import re
import sqlite3

DB_NAME 		= 'db/data.sqlite'
DB_TABLE 		= 'gigaapp_objectaddresses'

XLS_BOOK		= 'data/nn.xls'
XLS_SHEET		= 'vsp'

NN_OSB 	= 0
NN_VSP 	= 1
NN_OSBN = 3
NN_VSPN = 4
NN_TYPE = 5
NN_ADRR = 6
NN_PHON = 7
#===========================================
class IO ():

	def __init__(self):
		raise NotImplementedError
	
	def __del__(self):
		raise NotImplementedError

	def write(self):
		raise NotImplementedError
		

class FileIO(IO):

	def __init__(self,file):
		try:
			self.__file = codecs.open(file,'w', 'utf-8')  
			self.__OK	= True
		except:
			print 'Cant open file "%s"' % (file)
		
	def write(self, data=''):
		if self.__OK:
			self.__file.write("%s\n" % (data))
		
	def __del__(self):
		if self.__OK:
			self.__file.close()	
			
class SQLIO(IO):
	def __init__(self, dbName, tName = ''):
	
		self.__db = dbName
		
		try:
			self.__conn 	= sqlite3.connect(dbName)
			self.__c  		= self.__conn.cursor()
			#self.__c.execute('DELETE * FROM %s' % (tName))
			self.__OK		= True
			
		except:
			print 'Cant open databse %s' % (dbName)
			
	def write(self, data):
		if self.__OK:
			rez = self.__c.execute(data)	
		return rez
		
	def __del__(self):
		if self.__OK:
			self.__conn.commit()
			self.__c.close()
	
	def read(self):
		pass
#===========================================	
	
class DataSource:
	
	def __init__(self,data, **argw):
		raise NotImplementedError
	
	def data(self):
		raise NotImplementedError
		
	class _parce_adress:
		def __init__(self):
			self.__reset()
			

		def __reset(self):
			self.town = ''
			self.street = ''
			self.street_pref = ''
			self.number = ''
			self.organization = ''
			self.parce_error = False
	

		def do_parce(self, arg):
			self.__reset()
			
			if not arg:
				self.parce_error = True
				return
				
			self.data = arg
				
			split_data 	= re.split('\,', self.data, re.UNICODE)
			tmp_data 	= {}
		
			for key, val in enumerate(split_data):
				tmp_data[key] = val.strip()
			
			tmp_town 				= tmp_data[0]
		
			try:
				tmp_street 			= tmp_data[1]
			except:
				tmp_street 			= ''
		
			try:
				tmp_number 			= tmp_data[2]
			except:
				tmp_number 			= ''
		
			try:
				tmp_organization	= tmp_data[3]
			except:
				tmp_organization 	= ''
			
		
			self.town 			= re.sub(r'^(.[\.\s])','', tmp_town, re.UNICODE).strip()
		
			street_pattern		= re.compile(u"(ул[\.\,\s])|(пр[\.\,\s])|(тр[\.\,\s])", re.UNICODE | re.IGNORECASE )
		
			# в поле с улицей есть одно из: ул./пр./тр
			# считаем, что там валидный адрес и раскладываем распарсенные данные по переменным.
			if street_pattern.search(tmp_street):
				self.street 	  = street_pattern.sub('', tmp_street).strip()
				self.street_pref  = re.sub('\.','', street_pattern.search(tmp_street).group(0)).strip().lower()
				self.organization = tmp_organization.strip()
				self.number		  = tmp_number.strip()
			else:
				self.parce_error = True
				#print "Street parce error at: %s" % (self.data)
		
		
			
		def __str__(self):
			return u'%s|%s|%s|%s|%s' % (self.town, self.street, self.street_pref,self.number, self.organization )
#====================================================			
	
class Data_XLS(DataSource):
	def __init__(self, data, sheet):
		# Локальный парсер адреса.
		self.pa = self._parce_adress()

		self._filename = data
		self._xls_book		= xlrd.open_workbook(self._filename, formatting_info=True, )
		self._sheet 		= self._xls_book.sheet_by_name(sheet)
		
	def data(self):	
		for d in self.__import_xls():
			if self.__DataCheck(d):
				# пытаемся спарсить строчку содержащую адрес
				self.pa.do_parce(d[NN_ADRR])
				yield (d, (self.pa.town, self.pa.street, self.pa.street_pref, self.pa.number,self.pa.organization))
		

	def __import_xls(self):
	
	
		def convert_to_str(arg):
			if type(arg) == float:
				return str(int(arg))
			if type(arg) == str:
				return arg.strip()
			return arg

		for rownum in range(self._sheet.nrows):
			row 		= self._sheet.row_values(rownum)
			ret_data 	= []
		
			for c_el in row:
				ret_data.append(convert_to_str(c_el))
			
			yield ret_data			
		
	def __DataCheck(self, data):
		if '' != data[NN_TYPE]  and 'Тип ВСП' != data[NN_TYPE]: # skip headers and empty ВСП types
			if data[NN_ADRR]: # Адрес не пустой
				return True
		return False
#====================================================

class ShowData(object):
	def __init__(self, data=''):
		self._data = data
	
	#@property
	def data(self):
		raise NotImplementedError
		
		
class ShowDataSQL(ShowData):

	@property
	def data(self):
		
		sql = """INSERT INTO  '%s' (address_unparced, address_town, address_street, address_street_pref, address_number, address_organization, vsp_number, branch_number_old, branch_number,	vsp_type ) VALUES ('%s', '%s','%s','%s','%s','%s','%s','%s','%s','%s')""" % (	DB_TABLE, 
									self._data[0][NN_ADRR], 
									self._data[1][0], 
									self._data[1][1], 
									self._data[1][2], 
									self._data[1][3], 
									self._data[1][4], 
									self._data[0][NN_OSB], 
									self._data[0][NN_VSP], 
									self._data[0][NN_VSPN],
									self._data[0][NN_TYPE]								
									)
									
		return re.sub('\n', '', sql)
		
		
# A dumb class			
class ShowDataTxt(ShowData):
	@property
	def data(self):
		answ1 = '|'.join(self._data[0])
		answ2 = ' > '.join(self._data[1])
		return '%s, Parced address: %s' % (answ1, answ2)

#===========================================		
class appl(object):

		@staticmethod	
		def run():
			
			ff = FileIO('log/file.txt')
			fs = FileIO('log/file.sql')
			xls = Data_XLS(XLS_BOOK, XLS_SHEET)		
			sqls = SQLIO(DB_NAME,DB_TABLE)
			for d in xls.data():
				sh_sql = ShowDataSQL(d)
				sh_txt = ShowDataTxt(d)
				
				sqls.write(sh_sql.data)
				
				ff.write(sh_txt.data)
				fs.write(sh_sql.data)

if __name__ == '__main__':
	appl.run()
	