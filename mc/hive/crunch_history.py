import inspect, os
from hour_cruncher import HourCruncher



def main():
	h = HourCruncher(currentdir)
	h.crunch_past(24*30)

if __name__ == '__main__':
	main()