import sys
import psycopg2
import datetime
from py_linq import Enumerable
import xlwt 
from xlwt import Workbook 
import argparse
import requests
import os
from io import BytesIO

version = 6
#print(f'Version {version}')

statlineQuery = """
	select 		max(pitcherid) as pitcherid,
				pitchermlbamid,
				max(pitchername) as pitchername,
				sum(numoutsonplay) as numoutsonplay,
				sum(numruns) as numruns,
				sum(hit) as hit,
				sum(strikeout) as strikeout,
				sum(walk) as walk,
				sum(intentional_walk) as intentional_walk,
				sum(hbp) as hbp,
				max(decision) as decision,
				max(location) as location,
				max(opponent) as opponent
	from 
	(
	select 	actions.ghuid,
			case when actions.pitcherteamid = game_detail.hometeamid then 'vs' else '@' end as location,
			case when actions.pitcherteamid = game_detail.hometeamid then visitor.abbreviation else home.abbreviation end as opponent,
			COALESCE(pitches.pitchermlbamid, case when pitches.gid is null and actions.numoutsonplay > 0 and COALESCE(actions.defmlbam, '') != '' then cast(split_part(actions.defmlbam, ',', 1) as INTEGER) else -1 end) as pitchermlbamid,
			pitches.pitcherid,
			pitches.pitchername,
			pitches.hittermlbamid,
			pitches.hitterid,
			pitches.hittername,
			case when actions.numoutsonplay is not null then actions.numoutsonplay else 0 end as numoutsonplay,
			coalesce((select sum(case when runs.unearnedrun is null or runs.unearnedrun then 0 else 1 end) from runs where runs.onbasegid = actions.gid and not coalesce(runs.deleted, false)), 0) as numruns,
			case when actions.primaryevent in ('S', 'D', 'T', 'HR') then 1 else 0 end as hit,
			case when actions.primaryevent in ('K', 'KC', 'KS') then 1 else 0 end as strikeout,
			case when actions.primaryevent = 'BB' then 1 else 0 end as walk,
			case when actions.primaryevent = 'IBB' then 1 else 0 end as intentional_walk, 
			case when actions.primaryevent = 'HBP' then 1 else 0 end as hbp,
			case 	when pitches.pitcherid = game_detail.winningpitcherid then 'W'
					when pitches.pitcherid = game_detail.losingpitcherid then 'L'
					else 'ND' 		
			end as decision
	from actions 
	left join pitches on actions.gid = pitches.gid
	inner join schedule on schedule.ghuid = actions.ghuid 
	inner join game_detail on game_detail.ghuid = schedule.ghuid
	inner join teams home on schedule.home = home.ie_abbreviation
	inner join teams visitor on schedule.visitor = visitor.ie_abbreviation
	where (case when schedule.continuationdate is null then schedule.game_date
               	when schedule.continuationdate is not null and comments = 'PPD' then schedule.continuationdate
                when schedule.continuationdate is not null and comments = 'SUSP' then schedule.game_date
                else schedule.game_date
    		end) = %s

	and (COALESCE(pitches.pitchermlbamid, case when pitches.gid is null and actions.numoutsonplay > 0 and COALESCE(actions.defmlbam, '') != '' then cast(split_part(actions.defmlbam, ',', 1) as INTEGER) else -1 end) = game_detail.homestartingpitchermlbamid or COALESCE(pitches.pitchermlbamid, case when pitches.gid is null and actions.numoutsonplay > 0 and COALESCE(actions.defmlbam, '') != '' then cast(split_part(actions.defmlbam, ',', 1) as INTEGER) else -1 end) = game_detail.visitorstartingpitchermlbamid )
	) as f
	group by 	f.pitchermlbamid;
	"""

groupedPitchesQuery = """
	select	pitchermlbamid,
			pitchername,
			pitchtype,
			sum(num_pitches) as num_pitches,
			coalesce(sum(total_velo), 0) as total_velo,
			sum(num_velo) as num_velo,
			sum(num_called_strike) as num_called_strike,
			sum(num_whiff) as num_whiff,
			sum(num_foul) as num_foul	
	from pl_leaderboard_v2_daily
	where game_played = %s
	group by 	pitchermlbamid,
				pitchername,
				pitchtype;
    """
class StartingPitcherRoundupRecord:
	PitcherMLBID = 0
	PitcherInsideEdgeID = 0
	PitcherName = ""
	Location = ""
	Opponent = ""

	OutsRecorded = ""
	EarnedRuns = 0
	HitsAllowed = 0
	Walks = 0
	Strikeouts = 0
	IntentionalWalks = 0
	HitByPitch = 0
	Decision = ""

	PitchesThrown = 0
	PitchesWhiffs = 0
	PitchesCSW = 0
	PitchesCSWPercentage = 0

	FastballsThrown = 0
	FastballsTotalVelocity = 0
	FastballsVelocityCount = 0
	FastballsAverageVelocity = 0
	FastballsWhiffs = 0
	FastballsCSW = 0
	FastballsCSWPercentage = 0

	ChangeupsThrown = 0
	ChangeupsTotalVelocity = 0
	ChangeupsVelocityCount = 0
	ChangeupsAverageVelocity = 0
	ChangeupsWhiffs = 0
	ChangeupsCSW = 0
	ChangeupsCSWPercentage = 0

	CurveballsThrown = 0
	CurveballsTotalVelocity = 0
	CurveballsVelocityCount = 0
	CurveballsAverageVelocity = 0
	CurveballsWhiffs = 0
	CurveballsCSW = 0
	CurveballsCSWPercentage = 0

	SlidersThrown = 0
	SlidersTotalVelocity = 0
	SlidersVelocityCount = 0
	SlidersAverageVelocity = 0
	SlidersWhiffs = 0
	SlidersCSW = 0
	SlidersCSWPercentage = 0

	CuttersThrown = 0
	CuttersTotalVelocity = 0
	CuttersVelocityCount = 0
	CuttersAverageVelocity = 0
	CuttersWhiffs = 0
	CuttersCSW = 0
	CuttersCSWPercentage = 0

	SplittersThrown = 0
	SplittersTotalVelocity = 0
	SplittersVelocityCount = 0
	SplittersAverageVelocity = 0
	SplittersWhiffs = 0
	SplittersCSW = 0
	SplittersCSWPercentage = 0

	OthersThrown = 0
	OthersTotalVelocity = 0
	OthersVelocityCount = 0
	OthersAverageVelocity = 0
	OthersWhiffs = 0
	OthersCSW = 0
	OthersCSWPercentage = 0

def MatchingPitchData(groupedPitches, pitcher):
    return Enumerable(groupedPitches).where(lambda x: x[0] == pitcher.PitcherMLBID)

parser = argparse.ArgumentParser()
parser.add_argument("--date", help="date used for the script. If one is not selected, it uses yesterdays date by default")
args = parser.parse_args()

if args.date is not None:
	scriptDate = datetime.datetime.strptime(args.date, '%Y-%m-%d').date()
else:
	yesterday = datetime.date.today() - datetime.timedelta(days=1)
	scriptDate = yesterday

print(f'Generating SP Roundup data for {scriptDate}')
#print()

try:

    #connection = psycopg2.connect(user = os.getenv('PL_DB_USER'),
                                  #password = os.getenv('PL_DB_PW'),
                                  #host = "salty-kumquat.db.elephantsql.com",
                                  #port = "5432",
                                  #database = "pitcher-list")
    connection = psycopg2.connect(user = "pitcherlist",
										password = "4^xN2M6RxLsu*4J",
										host = "salty-kumquat.db.elephantsql.com",
										port = "5432",
										database = "pitcher-list")
    cursor = connection.cursor()
    # Print PostgreSQL Connection properties
    #print('Loading player game logs')
    cursor.execute(statlineQuery, (scriptDate,))
    pitchers = cursor.fetchall()

    #print('Loading player pitch logs')
    cursor.execute(groupedPitchesQuery, (scriptDate,))
    groupedPitches = cursor.fetchall()

except (Exception, psycopg2.Error) as error :
    print ("Error while connecting to PostgreSQL", error)
    sys.exit()
finally:
    #closing database connection.
        if(connection):
            cursor.close()
            connection.close()

StartingPitcherRoundupRecords = list()
#print('Compiling player data')
for pitcher in pitchers:
	record = StartingPitcherRoundupRecord()
	record.PitcherInsideEdgeID = pitcher[0]
	record.PitcherMLBID = pitcher[1]
	names = pitcher[2].split(", ")
	record.PitcherName = f'{names[1]} {names[0]}'
	innings, outs= divmod(pitcher[3], 3)
	record.OutsRecorded = f'{innings}.{outs}'
	record.EarnedRuns = pitcher[4]
	record.HitsAllowed = pitcher[5]
	record.Strikeouts = pitcher[6]
	record.Walks = pitcher[7]
	record.Decision = pitcher[10]
	record.Location = pitcher[11]
	record.Opponent = pitcher[12]

	matchingPitches = MatchingPitchData(groupedPitches, record)
	for pitch in matchingPitches:
		if pitch[2] == 'FA' or pitch[2] == 'SI' or pitch[2] == 'FT':
			record.FastballsThrown += pitch[3]
			record.FastballsTotalVelocity += pitch[4]
			record.FastballsVelocityCount += pitch[5]
			record.FastballsWhiffs += pitch[7]
			record.FastballsCSW += (pitch[6] + pitch[7])
		elif pitch[2] == 'CH':
			record.ChangeupsThrown += pitch[3]
			record.ChangeupsTotalVelocity += pitch[4]
			record.ChangeupsVelocityCount += pitch[5]
			record.ChangeupsWhiffs += pitch[7]
			record.ChangeupsCSW += (pitch[6] + pitch[7])
		elif pitch[2] == 'CU' or pitch[2] == 'KC':
			record.CurveballsThrown += pitch[3]
			record.CurveballsTotalVelocity += pitch[4]
			record.CurveballsVelocityCount += pitch[5]
			record.CurveballsWhiffs += pitch[7]
			record.CurveballsCSW += (pitch[6] + pitch[7])
		elif pitch[2] == 'SL':
			record.SlidersThrown += pitch[3]
			record.SlidersTotalVelocity += pitch[4]
			record.SlidersVelocityCount += pitch[5]
			record.SlidersWhiffs += pitch[7]
			record.SlidersCSW += (pitch[6] + pitch[7])
		elif pitch[2] == 'FC' or pitch[2] == 'CT':
			record.CuttersThrown += pitch[3]
			record.CuttersTotalVelocity += pitch[4]
			record.CuttersVelocityCount += pitch[5]
			record.CuttersWhiffs += pitch[7]
			record.CuttersCSW += (pitch[6] + pitch[7])
		elif pitch[2] == 'FS' or pitch[2] == 'SP':
			record.SplittersThrown += pitch[3]
			record.SplittersTotalVelocity += pitch[4]
			record.SplittersVelocityCount += pitch[5]
			record.SplittersWhiffs += pitch[7]
			record.SplittersCSW += (pitch[6] + pitch[7])
		else:
			record.OthersThrown += pitch[3] or 0
			record.OthersTotalVelocity += pitch[4] or 0
			record.OthersVelocityCount += pitch[5] or 0
			record.OthersWhiffs += pitch[7] or 0
			record.OthersCSW += (pitch[6] or 0) + (pitch[7] or 0)

	record.PitchesThrown = record.FastballsThrown + record.ChangeupsThrown + record.CurveballsThrown + record.SlidersThrown + record.CuttersThrown + record.SplittersThrown + record.OthersThrown
	record.PitchesCSW = record.FastballsCSW + record.ChangeupsCSW + record.CurveballsCSW + record.SlidersCSW + record.CuttersCSW + record.SplittersCSW + record.OthersCSW
	record.PitchesWhiffs = record.FastballsWhiffs + record.ChangeupsWhiffs + record.CurveballsWhiffs + record.SlidersWhiffs + record.CuttersWhiffs + record.SplittersWhiffs + record.OthersWhiffs
	record.PitchesCSWPercentage = (record.PitchesCSW / record.PitchesThrown) * 100 if record.PitchesThrown > 0 else 0
	record.FastballsCSWPercentage = (record.FastballsCSW / record.FastballsThrown) * 100 if record.FastballsThrown > 0 else 0
	record.ChangeupsCSWPercentage = (record.ChangeupsCSW / record.ChangeupsThrown) * 100 if record.ChangeupsThrown > 0 else 0
	record.CurveballsCSWPercentage = (record.CurveballsCSW / record.CurveballsThrown) * 100 if record.CurveballsThrown > 0 else 0
	record.SlidersCSWPercentage = (record.SlidersCSW / record.SlidersThrown) * 100 if record.SlidersThrown > 0 else 0
	record.CuttersCSWPercentage = (record.CuttersCSW / record.CuttersThrown) * 100 if record.CuttersThrown > 0 else 0
	record.SplittersCSWPercentage = (record.SplittersCSW / record.SplittersThrown) * 100 if record.SplittersThrown > 0 else 0
	record.OthersCSWPercentage = (record.OthersCSW / record.OthersThrown) * 100 if record.OthersThrown > 0 else 0

	record.FastballsAverageVelocity = (record.FastballsTotalVelocity/ record.FastballsVelocityCount) if record.FastballsVelocityCount > 0 else 0
	record.ChangeupsAverageVelocity = (record.ChangeupsTotalVelocity / record.ChangeupsVelocityCount) if record.ChangeupsVelocityCount > 0 else 0
	record.CurveballsAverageVelocity = (record.CurveballsTotalVelocity / record.CurveballsVelocityCount) if record.CurveballsVelocityCount > 0 else 0
	record.SlidersAverageVelocity = (record.SlidersTotalVelocity / record.SlidersVelocityCount) if record.SlidersVelocityCount > 0 else 0
	record.CuttersAverageVelocity = (record.CuttersTotalVelocity / record.CuttersVelocityCount) if record.CuttersVelocityCount > 0 else 0
	record.SplittersAverageVelocity = (record.SplittersTotalVelocity / record.SplittersVelocityCount) if record.SplittersVelocityCount > 0 else 0
	record.OthersAverageVelocity = (record.OthersTotalVelocity / record.OthersVelocityCount) if record.OthersVelocityCount > 0 else 0
	
	StartingPitcherRoundupRecords.append(record)

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
sheet1.write(1, 0, f'<p>Let\'s see how every other SP did {scriptDate.strftime("%A")}:</p>')

sheet1.write(0, 8, 'Total', style1) 
sheet1.write(0, 11, 'FB', style1) 
sheet1.write(0, 14, 'CH', style1) 
sheet1.write(0, 17, 'CU', style1) 
sheet1.write(0, 20, 'SL', style1) 
sheet1.write(0, 23, 'FC', style1) 
sheet1.write(0, 26, 'FS', style1) 
sheet1.write(0, 29, 'Other', style1) 


sheet1.write(1, 1, 'Player', style2) 
sheet1.write(1, 2, 'IP', style2) 
sheet1.write(1, 3, 'ER', style2) 
sheet1.write(1, 4, 'H', style2) 
sheet1.write(1, 5, 'K', style2) 
sheet1.write(1, 6, 'BB', style2) 
sheet1.write(1, 7, 'Dec', style2) 
sheet1.write(1, 8, '#', style2) 
sheet1.write(1, 9, 'CSW', style2) 
sheet1.write(1, 10, 'CSW%', style2)
sheet1.write(1, 11, '#', style2) 
sheet1.write(1, 12, 'MPH', style2) 
sheet1.write(1, 13, 'CSW%', style2)
sheet1.write(1, 14, '#', style2) 
sheet1.write(1, 15, 'MPH', style2) 
sheet1.write(1, 16, 'CSW%', style2)
sheet1.write(1, 17, '#', style2) 
sheet1.write(1, 18, 'MPH', style2) 
sheet1.write(1, 19, 'CSW%', style2)
sheet1.write(1, 20, '#', style2) 
sheet1.write(1, 21, 'MPH', style2) 
sheet1.write(1, 22, 'CSW%', style2)
sheet1.write(1, 23, '#', style2) 
sheet1.write(1, 24, 'MPH', style2) 
sheet1.write(1, 25, 'CSW%', style2)
sheet1.write(1, 26, '#', style2) 
sheet1.write(1, 27, 'MPH', style2) 
sheet1.write(1, 28, 'CSW%', style2)
sheet1.write(1, 29, '#', style2) 
sheet1.write(1, 30, 'MPH', style2) 
sheet1.write(1, 31, 'CSW%', style2)

index = 2

gallowsPolePlayers =  Enumerable(StartingPitcherRoundupRecords).order_by_descending(lambda x : x.PitchesWhiffs).group_by(key_names=['whiffs'], key=lambda x: str(x.PitchesWhiffs)).first_or_default().select(lambda x: x.PitcherMLBID)
kingColePlayers =  Enumerable(StartingPitcherRoundupRecords).where(lambda x: x.OutsRecorded > "5").order_by_descending(lambda x : x.PitchesCSWPercentage).group_by(key_names=['whiffs'], key=lambda x: str(x.PitchesCSWPercentage)).first_or_default().select(lambda x: x.PitcherMLBID)

for player in Enumerable(StartingPitcherRoundupRecords).order_by(lambda x: x.EarnedRuns).then_by_descending(lambda x: x.OutsRecorded).then_by_descending(lambda x: x.PitchesCSW).to_list():
	if "Nola" in player.PitcherName and "Aaron" in player.PitcherName:
		blurb = "WAKEUP"
	else:
		blurb = "BLURB"

	if player.PitcherMLBID in gallowsPolePlayers:
		whiffsline = f'<span class="highlight" style="color: #cb7e1f;">{player.PitchesWhiffs} Whiffs</span>'
	else:
		whiffsline = f'{player.PitchesWhiffs} Whiffs'

	if player.PitcherMLBID in kingColePlayers:
		cswLine = f'<span class="highlight" style="color: #cb7e1f;">{str(round(player.PitchesCSWPercentage, 0))}% CSW</span>'
	else:
		cswLine = f'{str(round(player.PitchesCSWPercentage, 0))}% CSW'

	statline = f'{player.OutsRecorded} IP, {player.EarnedRuns} ER, {player.HitsAllowed} Hits, {player.Walks} BBs, {player.Strikeouts} Ks - {whiffsline}, {cswLine}'
	sheet1.write(index, 0, f'<p class="pitcher-blurb"><strong><span class="pitcher-name">{player.PitcherName}</span><span> {player.Location} {player.Opponent} ({player.Decision})</span><span> - </span><span class="pitcher-stats">{statline}</span><span>. </span></strong> {blurb} </p>')
	
	sheet1.write(index, 1, player.PitcherName) 
	sheet1.write(index, 2, player.OutsRecorded) 
	sheet1.write(index, 3, player.EarnedRuns) 
	sheet1.write(index, 4, player.HitsAllowed) 
	sheet1.write(index, 5, player.Strikeouts) 
	sheet1.write(index, 6, player.Walks) 
	sheet1.write(index, 7, player.Decision) 

	sheet1.write(index, 8, player.PitchesThrown) 
	sheet1.write(index, 9, player.PitchesCSW) 
	sheet1.write(index, 10, str(round(player.PitchesCSWPercentage, 1)))
	sheet1.write(index, 11, player.FastballsThrown) 
	sheet1.write(index, 12, str(round(player.FastballsAverageVelocity, 1))) 
	sheet1.write(index, 13, str(round(player.FastballsCSWPercentage, 1)))
	sheet1.write(index, 14, player.ChangeupsThrown) 
	sheet1.write(index, 15, str(round(player.ChangeupsAverageVelocity, 1))) 
	sheet1.write(index, 16, str(round(player.ChangeupsCSWPercentage, 1)))
	sheet1.write(index, 17, player.CurveballsThrown) 
	sheet1.write(index, 18, str(round(player.CurveballsAverageVelocity, 1))) 
	sheet1.write(index, 19, str(round(player.CurveballsCSWPercentage, 1)))
	sheet1.write(index, 20, player.SlidersThrown) 
	sheet1.write(index, 21, str(round(player.SlidersAverageVelocity, 1))) 
	sheet1.write(index, 22, str(round(player.SlidersCSWPercentage, 1)))
	sheet1.write(index, 23, player.CuttersThrown) 
	sheet1.write(index, 24, str(round(player.CuttersAverageVelocity, 1))) 
	sheet1.write(index, 25, str(round(player.CuttersCSWPercentage, 1)))
	sheet1.write(index, 26, player.SplittersThrown) 
	sheet1.write(index, 27, str(round(player.SplittersAverageVelocity, 1))) 
	sheet1.write(index, 28, str(round(player.SplittersCSWPercentage, 1)))
	sheet1.write(index, 29, player.OthersThrown) 
	sheet1.write(index, 30, str(round(player.OthersAverageVelocity, 1))) 
	sheet1.write(index, 31, str(round(player.OthersCSWPercentage, 1)))

	index += 1
wb.save(f'SPRoundup{scriptDate}.xls') 


#print("Done!")
sys.exit()