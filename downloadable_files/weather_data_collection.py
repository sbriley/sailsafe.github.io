import requests
import cStringIO
import re
import csv


YEAR = 0
DOY = 1
UTC = 2
AIRTEMP = 3
WINDSPEED = 4
WINDGUST = 5
WINDDIR = 6


def DOY_from_MDY(month,day,year):
    days_in_month = [31,28,31,30,31,30,31,31,30,31,30]
    start_of_month = []
    start_of_month.append(0)
    for i in days_in_month:
        if i == 28 and year % 4 == 0:
            i = i+1
        start_of_month.append(i+start_of_month[-1])
    return start_of_month[month - 1] + day

def add_single_point_to_dictionary(d,datapoint):
    year = int(datapoint[YEAR])
    doy = int(datapoint[DOY])
    utc = int(datapoint[UTC])
    dataval = {'airtemp': datapoint[AIRTEMP],
    'windspeed': datapoint[WINDSPEED],
    'windgust': datapoint[WINDGUST],
    'winddir': datapoint[WINDDIR]}
    if(year in d and doy in d[year]):
        d[year][doy][utc] = dataval
    elif year in d:
        d[year][doy] = {utc: dataval}
    else:
        d[year] = {doy: {utc: dataval}}

def archive_data_source(year):
    return requests.get('https://www.glerl.noaa.gov//metdata/chi/archive/chi'+str(year)+'.04t.txt').text

def current_year_data_source(month, day):
    year = 2018
    if day < 10:
        day = '0'+str(day)
    else:
        day = str(day)
    if month < 10:
        month = '0'+str(month)
    else:
        month = str(month)
    return requests.get('https://www.glerl.noaa.gov//metdata/chi/'+str(year)+'/'+str(year)+month+day+'.04t.txt').text


def add_data_from_source(d, stringsource):
    buf = cStringIO.StringIO(stringsource)

    lines = 0
    for line in buf:
        # if lines > maxlines:
        #     break
        match = re.search("[\d]{4}\s.*$",line)
        if match != None:
            add_single_point_to_dictionary(d,match.group().split())

        # print x
        lines = lines + 1

def populate_dictionary(d):
    add_data_from_source(d,archive_data_source(2015))
    add_data_from_source(d,archive_data_source(2016))
    add_data_from_source(d,archive_data_source(2017))



def get_visibility_and_precipitation(month,day,year):
    vis =  re.search('''Visibility[<]/span></td>
		<td>
  <span class="wx-data"><span class="wx-value">[\d\.]+[\D]''',requests.get('https://www.wunderground.com/history/airport/KPWK/'+str(year)+'/'+str(month)+'/'+str(day)+'/DailyHistory.html').text).group()
    pre =  re.search('''Precipitation[<]/span></td>
		<td>
  <span class="wx-data"><span class="wx-value">[\d\.]+[\D]''',requests.get('https://www.wunderground.com/history/airport/KPWK/'+str(year)+'/'+str(month)+'/'+str(day)+'/DailyHistory.html').text)
    if pre == None:
        return (re.search("[\d.]+",vis).group(),0.0)
    else:
        return (re.search("[\d.]+",vis).group(),re.search("[\d.]+",pre.group()).group())

def make_attribute_string(attributenames):
    attributestring = ""
    attributestring = attributestring + '''@RELATION stuff

'''
    print("Appending names")
    for attributename_index in range(len(attributenames)-1):
        attributestring = attributestring + '''@ATTRIBUTE ''' + attributenames[attributename_index] + ''' numeric
'''

    attributestring = attributestring + '''@ATTRIBUTE practice_status {OTW, CT}

@DATA
'''
    return attributestring


def write_file(d):
    raw_data = open('nustcoach_storm.csv')
    attributenames = ['one','two']
    data = [['1','2']]
    fp = open("nustcoach_new_storm.csv",'w')





    print data.shape
    print "Creating file\n"
    fp.write(attributestring)
    for i_a in range(len(data)):
        print len(data[i])
        for j_a in range(40):
            fp.write("%d," % data[i_a][j_a])
        fp.write("%d" % data[i_a][-1])
        fp.write("\n")

d = {}
print("Populating Dictionary")
populate_dictionary(d)

lines = 0
maxlines = 10
raw_data = open('nustcoach_storm.csv')

attributenames = ['date','airtemp','windspeed','windgust','winddir','precipitation','visibility','storm_status','coach','practice_status']
data = []
reader = csv.DictReader(raw_data)
fp = open('nustdata.arff','w')
fp.write(make_attribute_string(attributenames))
print("Reading rows")
for row in reader:

    print("Row " + str(lines))
    # if lines > 10:
    #     break
    month = int(row['Month'])
    day = int(row['Day'])
    year = int(row['Year'])
    practice_status = row['Practice Status']
    datapoint_coach = row['Coach']
    datapoint_storm = row['Storm Status']

    if year == 2018:
        add_data_from_source(d,current_year_data_source(month,day))
    datapoint_airtemp = d[year][DOY_from_MDY(month,day,year)][1500+500]['airtemp']
    datapoint_windspeed = d[year][DOY_from_MDY(month,day,year)][1500+500]['windspeed']
    datapoint_windgust = d[year][DOY_from_MDY(month,day,year)][1500+500]['windgust']
    datapoint_winddir = d[year][DOY_from_MDY(month,day,year)][1500+500]['winddir']
    datapoint_visibility,datapoint_precipitation = get_visibility_and_precipitation(month,day,year)

    data.append([
    str(month)+'/'+str(day)+'/'+str(year),
    datapoint_airtemp,
    datapoint_windspeed,
    datapoint_windgust,
    datapoint_winddir,
    datapoint_precipitation,
    datapoint_visibility,
    datapoint_storm,
    datapoint_coach,
    practice_status
    ])



    for i in range(len(data[0])-1):
        fp.write("%s," % data[-1][i])
    fp.write("%s" % data[-1][-1])
    fp.write("\n")

    lines = lines + 1

print data
