import datetime


def convert_epoch_to_datetime(epoch_time):
	return datetime.datetime.fromtimestamp(epoch_time)


def convert_datetime_to_epoch(datetime_l):
	"""rounded to seconds"""
	return str(round(datetime_l.timestamp()))


def convert_epoch_to_cron_expression(epoch_time):
	datetime_l = convert_epoch_to_datetime(epoch_time)
	"""cron expression without specified weekday"""
	return "{0} {1} {2} {3} *".format(datetime_l.minute, datetime_l.hour, datetime_l.day, datetime_l.month)


def convert_datetime_to_cron_expression(datetime_l):
	"""cron expression without specified weekday"""
	return "{0} {1} {2} {3} *".format(datetime_l.minute, datetime_l.hour, datetime_l.day, datetime_l.month)

