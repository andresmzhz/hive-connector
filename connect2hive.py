# -*- coding: utf-8 -*-
"""
Created on Thu Sep 14 13:11:00 2017

@author: AndresM
"""

import os
import sys
import pyhive
from pyhive import hive


class Hive:
	"""
		Module for establishing a connection to a Hive Server 2
	"""

	def __init__(self, host, port, database):
		"""
			Class is initialized by establishing the connection.

		:param host: String with the direction of the Hive Server.
		:param port: Integer with the port used for the connection
		:param database: String with the name of the database within the Hive Server from which we want to get data.
		"""

		# Initializing the settings

		self.hivecur = None
		self.hiveconn = None
		self.hive_open = False

		# Collecting the credentials

		user, password = self.credentials()

		# Establishing the connection

		try:

			if user and password:

				self.hiveconn = hive.Connection(host=host, port=port, username=user, password=password, database=database)

		except pyhive.exc.OperationalError:

			print('Can not find indicated database ' + database + ' within indicated server ' + host)

		except ValueError:

				try:

					self.hiveconn = hive.Connection(host=host, port=port, username=user, password=password, database=database, auth='LDAP')

				except pyhive.exc.OperationalError:

					print('Can not find indicated database ' + database + ' within indicated server ' + host)

		if self.hiveconn:

			self.hive_open = True

	def close(self):
		"""
			Function for closing the established Hive Server connection

		:return:
		"""

		if self.hive_open:

			self.hiveconn.close()
			self.hive_open = False

	@staticmethod
	def credentials():
		"""
			Function for collecting the credentials required for connection

		:return: Two strings containing the username and password required for the connection.
		"""

		homedir = os.path.expanduser("~")

		if os.path.exists(os.path.join(os.sep, homedir, '.loginconfig')):

			sys.path.insert(0, os.path.join(os.sep, homedir, '.loginconfig'))
			import config

			# Credentials

			user = config.username
			pswd = config.password

			return user, pswd

		else:

			print('No credentials could be collected as no .loginconfig folder could be located within ' + homedir)

	def executequery(self, query):
		"""
			Function for executing a query using the established Hive Server connection

		:param query: String with the query to be implemented
		:return:
		"""

		if self.hive_open:

			# Opening a cursor

			self.hivecur = self.hiveconn.cursor()

			self.hivecur.execute(query)

		else:

			print('Connection to the Hive Server is not open.')

	def fetchall(self):
		"""
			Function to retrieve the results from the already executed query

		:return:
		"""

		if self.hive_open:

			if self.hivecur:

				return self.hivecur.fetchall()

			else:

				print('No cursor is open')

		else:

			print('Connection to Hive server is not open.')

	def saveresult(self, pathout):
		"""
			Function to save the results of the query implemented to a plain txt file

		:param pathout: Srting with the path to the file to be written
		:return:
		"""

		if self.hive_open and self.hivecur:

			with open(pathout, 'w') as fileout:

				for piece in self.hivecur.fetchall():

					# Each entry within the cursor is a tuple composed of the values for the parameters required in the query

					fileout.writelines(','.join([str(elem) for elem in piece]) + '\n')
