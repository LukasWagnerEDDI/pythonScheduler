import datetime


def convert_epoch_to_datetime(epoch_time):
	return datetime.datetime.fromtimestamp(float(epoch_time))


def initialize_datetime_from_string(datetime_l):
	datetime_arr = str(datetime_l).replace('-', ' ').replace(':', ' ').split()
	return datetime.datetime(int(datetime_arr[0]), int(datetime_arr[1]), int(datetime_arr[2]), int(datetime_arr[3]), int(datetime_arr[4]))


def convert_datetime_to_epoch(datetime_l):
	"""YYyy-MM-DD HH:MM"""
	if isinstance(datetime_l, str):
		datetime_l = initialize_datetime_from_string(datetime_l)
	
	return datetime_l.timestamp()


def convert_epoch_to_cron_expression(epoch_time):
	datetime_l = convert_epoch_to_datetime(epoch_time)
	"""cron expression without specified weekday"""
	return "{0} {1} {2} {3} *".format(datetime_l.minute, datetime_l.hour, datetime_l.day, datetime_l.month)


def convert_datetime_to_cron_expression(datetime_l):
	"""cron expression without specified weekday"""
	return "{0} {1} {2} {3} *".format(datetime_l.minute, datetime_l.hour, datetime_l.day, datetime_l.month)


convert_datetime_to_epoch("2022-09-27 14:44:00.004458")




