import sys
import psycopg2
import datetime
import argparse
from py_linq import Enumerable
import xlwt 
from xlwt import Workbook 
import requests

version = 2
#print(f'Version {version}')

statlineQuery = """
	select 	hittermlbamid,
			hittername,
			pitcherteam_abb,			
			hitter_home_away,
			sum(num_pa) as num_pa,
			sum(num_ab) as num_ab,
			sum(num_1b) as num_1b,
			sum(num_2b) as num_2b,
			sum(num_3b) as num_3b,
			sum(num_hr) as num_hr,
			sum(num_bb) as num_bb,
			sum(num_ibb) as num_ibb,
			sum(num_hbp) as num_hbp,
			sum(num_k) as num_k,
			sum(num_runs) as num_runs,
			hitterteam_abb
	from 
	(
	select	hittermlbamid,
			hittername,
			pitcherteam_abb,
			hitterteam_abb,
			hitter_home_away,
			num_pa,
			num_ab,
			num_1b,
			num_2b,
			num_3b,
			num_hr,
			num_bb,
			num_ibb,
			num_hbp,
			num_k,
			num_runs
	from pl_leaderboard_v2_daily
	where game_played = %s
	) as f
	group by 	f.hittermlbamid,
				f.hittername,
				f.pitcherteam_abb,
				f.hitterteam_abb,
				hitter_home_away
	;
"""

stolenBasesAndRunsQuery = """
	select 	f.hitterid,
			f.hittermlbamid,
			sum(f.runscored) as runscored,
			sum(f.stolenbase) as stolenbases,
			sum(f.caughtstealing) as caughtstealing
	from 
	(
	select 	actions.ghuid,
			case when (runs.runnerid is not null and runs.runnerid != 0) then runs.runnerid when (base_runners.sbid is not null and base_runners.sbid != 0) then base_runners.sbid when (base_runners.csid is not null and base_runners.csid != 0) is not null then base_runners.csid else 0 end as hitterid,
			case when (runs.runnermlbamid is not null and runs.runnermlbamid != 0) then runs.runnermlbamid when (base_runners.sbidmlbamid is not null and base_runners.sbidmlbamid != 0) then base_runners.sbidmlbamid when (base_runners.csidmlbamid is not null and base_runners.csidmlbamid != 0) then base_runners.csidmlbamid else 0 end as hittermlbamid,
			case when runs.runnerid is not null then 1 else 0 end as runscored,
			case when coalesce(base_runners.sbid, 0) != 0 then 1 else 0 end as stolenbase,
			case when coalesce(base_runners.csid, 0) != 0 then 1 else 0 end as caughtstealing
	from actions 
	left join runs on runs.gid = actions.gid
	left join base_runners on base_runners.gid = actions.gid
	inner join schedule on schedule.ghuid = actions.ghuid 
	inner join game_detail on game_detail.ghuid = schedule.ghuid
	inner join teams home on schedule.home = home.ie_abbreviation
	inner join teams visitor on schedule.visitor = visitor.ie_abbreviation
	where (case when schedule.continuationdate is null then schedule.game_date
               	when schedule.continuationdate is not null and comments = 'PPD' then schedule.continuationdate
                when schedule.continuationdate is not null and comments = 'SUSP' then schedule.game_date
                else schedule.game_date
    		end) = %s
	and (runs.gid is not null or base_runners.gid is not null)
	) as f
	group by 	f.hitterid,
				f.hittermlbamid;
"""

class BattersBoxRecord:
	HitterMLBID = 0
	HitterInsideEdgeID = 0
	HitterName = ""
	Location = ""
	Opponent = ""
	Team = ""

	PA = 0
	AB = 0
	Singles = 0
	Doubles = 0
	Triples = 0
	HomeRuns = 0
	Walks = 0
	IntentionalWalks = 0
	HitByPitch = 0
	Strikeouts = 0
	Runs = 0
	Hits = 0
	TotalBases = 0
	StolenBases = 0
	RunsBattedIn = 0

def MatchingStolenBaseAndRunRecord(records, hitter):
    return Enumerable(records).first_or_default(lambda x: x[1] == hitter.HitterMLBID)

try:
	parser = argparse.ArgumentParser()
	parser.add_argument("--date", help="date used for the script. If one is not selected, it uses yesterdays date by default")
	args = parser.parse_args()

	if args.date is not None:
		scriptDate = datetime.datetime.strptime(args.date, '%Y-%m-%d').date()
		#scriptDate = args.date
	else:
		yesterday = datetime.date.today() - datetime.timedelta(days=1)
		scriptDate = yesterday

	print(f'Generating Batters Box data for {scriptDate}')
	#print()


	try:

		connection = psycopg2.connect(user = "pitcherlist",
										password = "4^xN2M6RxLsu*4J",
										host = "salty-kumquat.db.elephantsql.com",
										port = "5432",
										database = "pitcher-list")

		cursor = connection.cursor()
		# Print PostgreSQL Connection properties

		#print('Loading player game logs')
		cursor.execute(statlineQuery, (scriptDate,))
		hitters = cursor.fetchall()

		cursor.execute(stolenBasesAndRunsQuery, (scriptDate,))
		stolenBasesAndRuns = cursor.fetchall()

	except (Exception, psycopg2.Error) as error :
		print("Error while connecting to PostgreSQL", error)
		sys.exit()
	finally:
		#closing database connection.
			if(connection):
				cursor.close()
				connection.close()

	BattersBoxRecords = list()
	#print('Compiling player data')
	for hitter in hitters:
		record = BattersBoxRecord()
		record.HitterMLBID = hitter[0]
		names = hitter[1].split(", ")
		record.HitterName = f'{names[1]} {names[0]}'
		record.Location = "vs" if hitter[3] == "Away" else "@"
		record.Opponent = hitter[2]
		record.PA = hitter[4]
		record.AB = hitter[5]
		record.Singles = hitter[6]
		record.Doubles = hitter[7]
		record.Triples = hitter[8]
		record.HomeRuns = hitter[9]
		record.Walks = hitter[10]
		record.IntentionalWalks = hitter[11]
		record.HitByPitch = hitter[12]
		record.Strikeouts = hitter[13]
		record.RunsBattedIn = hitter[14]
		record.Team = hitter[15]

		record.Hits = record.Singles + record.Doubles + record.Triples + record.HomeRuns
		record.TotalBases = record.Singles + (record.Doubles * 2) + (record.Triples * 3) + (record.HomeRuns * 4)

		matchingRecord = MatchingStolenBaseAndRunRecord(stolenBasesAndRuns, record)
		if matchingRecord is not None:
			record.Runs = matchingRecord[2]
			record.StolenBases = matchingRecord[3]
			record.CaughtStealing = matchingRecord[4]

		BattersBoxRecords.append(record)

	#print("Writing data to excel")
	# Workbook is created 
	wb = Workbook() 
  
	# add_sheet is used to create sheet. 
	sheet1 = wb.add_sheet('Sheet 1') 
	style1 = xlwt.easyxf('font: bold 1') 
	style2 = xlwt.easyxf('font: bold 1') 
	borders = xlwt.Borders()
	borders.bottom = xlwt.Borders.THIN
	style2.borders = borders

	sheet1.write(0, 0, scriptDate.strftime("%m/%d/%y"), style1)  
	sheet1.write(1, 0, 'Player', style2) 
	sheet1.write(1, 1, 'Statline', style2) 
	sheet1.write(1, 2, 'PA', style2) 
	sheet1.write(1, 3, 'AB', style2) 
	sheet1.write(1, 4, 'H', style2) 
	sheet1.write(1, 5, '1B', style2) 
	sheet1.write(1, 6, '2B', style2) 
	sheet1.write(1, 7, '3B', style2) 
	sheet1.write(1, 8, 'HR', style2) 
	sheet1.write(1, 9, 'BB', style2) 
	sheet1.write(1, 10, 'IBB', style2)
	sheet1.write(1, 11, 'HBP', style2) 
	sheet1.write(1, 12, 'K', style2) 
	sheet1.write(1, 13, 'R', style2)
	sheet1.write(1, 14, 'RBI', style2)
	sheet1.write(1, 15, 'SB', style2)

	sheet1.write(1, 17, f'<p>Let\'s see how every other hitter did {scriptDate.strftime("%A")}:</p>')

	index = 2
	for player in Enumerable(BattersBoxRecords).order_by_descending(lambda x: x.TotalBases + x.StolenBases + x.Runs + x.RunsBattedIn - x.Strikeouts).to_list():
		sheet1.write(index, 0, player.HitterName) 

		hits = f'{player.Hits}-{player.AB}'
		homeruns = f'{player.HomeRuns} HR' if player.HomeRuns > 1  else ('HR' if player.HomeRuns == 1 else "")
		doubles = f'{player.Doubles} 2B' if player.Doubles > 1  else ('2B' if player.Doubles == 1 else "")
		triples = f'{player.Triples} 3B' if player.Triples > 1  else ('3B' if player.Triples == 1 else "")
		rbi = f'{player.RunsBattedIn} RBI' if player.RunsBattedIn > 1  else ('RBI' if player.RunsBattedIn == 1 else "")
		runs = f'{player.Runs} R' if player.Runs > 1  else ('R' if player.Runs == 1 else "")
		walks = f'{player.Walks} BB' if player.Walks > 1  else ('BB' if player.Walks == 1 else "")
		stolenbases = f'{player.StolenBases} SB' if player.StolenBases > 1  else ('SB' if player.StolenBases == 1 else "")

		statline = hits

		if homeruns != "":
			statline += f', {homeruns}'
		if doubles != "":
			statline += f', {doubles}'
		if triples != "":
			statline += f', {triples}'
		if runs != "":
			statline += f', {runs}'
		if rbi != "":
			statline += f', {rbi}'
		if walks != "":
			statline += f', {walks}'
		if stolenbases != "":
			statline += f', {stolenbases}'

		sheet1.write(index, 1, statline) 
		sheet1.write(index, 2, player.PA) 
		sheet1.write(index, 3, player.AB) 
		sheet1.write(index, 4, player.Hits) 
		sheet1.write(index, 5, player.Singles) 
		sheet1.write(index, 6, player.Doubles) 
		sheet1.write(index, 7, player.Triples) 

		sheet1.write(index, 8, player.HomeRuns) 
		sheet1.write(index, 9, player.Walks) 
		sheet1.write(index, 10, player.IntentionalWalks)
		sheet1.write(index, 11, player.HitByPitch) 
		sheet1.write(index, 12, player.Strikeouts) 
		sheet1.write(index, 13, player.Runs)
		sheet1.write(index, 14, player.RunsBattedIn) 
		sheet1.write(index, 15, player.StolenBases)

		sheet1.write(index, 17, f'<div class="hitter-blurb" data-player-id="{player.HitterMLBID}"><strong><span class="hitter-name">{player.HitterName}</span><span> (POS, {player.Team})</span><span> - </span><span class="hitter-stats">{statline}</span><span> - </span></strong> BLURB </div>')
		index += 1
  
	wb.save(f'BattersBox{scriptDate}.xls') 

        
	#print("Done!")

except:
	print("Task Failed")

sys.exit()


