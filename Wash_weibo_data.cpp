#include "Wash_weibo_data.h"
#include <fstream>
#include <string>
#include <sstream>
#include <cstdlib>
#include <set>
#include <map>
#include <utility>

using namespace BasicPath;

// ====================================== Time ============================================== //
bool operator<(const WeiboTime &l, const WeiboTime &r)
{
	if (l.year != r.year)
		return l.year < r.year;
	else if (l.month != r.month)
		return l.month < r.month;
	else if (l.day != r.day)
		return l.day < r.day;
	else if (l.hour != r.hour)
		return l.hour < r.hour;
	else if (l.minute != r.minute)
		return l.minute < r.minute;
	else if (l.second != r.second)
		return l.second < r.second;
	else
		return false;
}
bool operator==(const WeiboTime &l, const WeiboTime &r)
{
	if (l.year == r.year && l.month == r.month && l.day == r.day && l.hour == r.hour && l.minute == r.minute && l.second == r.second)
		return true;
	else
		return false;
}

/*
*	write to ostream
*/
std::ostream& operator<<(std::ostream &os, const WeiboTime &u)
{
	os << u.year << " " << u.month << " " << u.day << " " << u.hour << " " << u.minute << " " << u.second;
	return os;
}

WeiboTime::WeiboTime() : year(0), month(0), day(0), hour(0), minute(0), second(0)
{ }
WeiboTime::WeiboTime(unsigned a, unsigned b, unsigned c, unsigned d, unsigned e, unsigned f) : year(a), month(b), day(c), hour(d), minute(e), second(f)
{ }
WeiboTime::WeiboTime(const WeiboTime &u) : year(u.year), month(u.month), day(u.day), hour(u.hour), minute(u.minute), second(u.second)
{ }
WeiboTime& WeiboTime::operator=(const WeiboTime &u)
{
	if (this == &u)
		return *this;
	year = u.year, month = u.month, day = u.day, hour = u.hour, minute = u.minute, second = u.second;
	return *this;
}
bool WeiboTime::setContri(std::vector<unsigned> &vec)
{
	if (vec.size() != 6)
		return false;
	year = vec[0], month = vec[1], day = vec[2], hour = vec[3], minute = vec[4], second = vec[5];
	return true;
}

// ================================= WeiboDataWash ========================================== //

/*
*	step one :
*	extract from : ./data_origin/event_origin/
*	save to : ./data_washed/event_extract/ && ./data_washed/event_extract_with_time/
*/
bool WeiboDataWash::runStepOne()
{
	if (processOriginEventExtractEdge(BasicPath::OriginEventFilePrefix, BasicPath::EventExtractPrefix, BasicPath::EventExtractWithTimePrefix) == false) {
		std::cerr << "Weibo::runStepOne() error." << std::endl;
		return false;
	}
	return true;
}
bool WeiboDataWash::processOriginEventExtractEdge(const std::string &sourcePrefix, const std::string &targetPrefix, const std::string &targetWithTimePrefix)
{
	//std::cout << "=== Process Origin Event Files... ===" << std::endl;

	for (const auto &file : BasicPath::vecEventOriginFiles) {
		std::ifstream fileInput(sourcePrefix + file, std::ios_base::in);
		std::ofstream fileOutNoTime(targetPrefix + file, std::ios_base::out);
		std::ofstream fileOutWithTime(targetWithTimePrefix + file, std::ios_base::out);

		if (!fileInput.is_open() || !fileOutNoTime.is_open() || !fileOutWithTime.is_open())
			return false;

		long long lineCounter = 0;
		std::string tempString;

		while (getline(fileInput, tempString)) {
			if (lineCounter++ % 1000000 == 0)
				std::cout << OriginEventFilePrefix + file << " : " << lineCounter << std::endl;				//print

			std::vector<std::string> vecUid;
			std::string rtUid;
			WeiboTime timeObj;

			if (extractUidRtUidFromString(tempString, vecUid, rtUid) == false)
				continue;
			if (vecUid.size() == 0 || rtUid.empty() == true)
				continue;
			if (extractTimeFromString(tempString, timeObj) == false)
				continue;

			// handle the edge in event trans network
			// uid:D E C    rtuid:J
			// the link of trans: J->C->E->D 就是三条边 
			try {
				auto uid_it_pre = vecUid.begin();
				if (uid_it_pre == vecUid.end()) {
					//std::cerr << "set uid is empty, string is : " << tempString << std::endl;
					continue;
				}
				auto uid_it_aft = uid_it_pre + 1;
				if (uid_it_aft == vecUid.end()) {
					fileOutNoTime << rtUid << "\t" << (*uid_it_pre) << "\n";					//only one edge
					fileOutWithTime << rtUid << "\t" << (*uid_it_pre) << "\t" << timeObj << "\n";
					continue;
				}
				else {																			//several edges
					fileOutNoTime << rtUid << "\t" << (*uid_it_pre) << "\n";
					fileOutWithTime << rtUid << "\t" << (*uid_it_pre) << "\t" << timeObj << "\n";

					uid_it_pre = uid_it_aft;
					uid_it_aft++;
					while (uid_it_aft != vecUid.end()) {
						fileOutNoTime << (*uid_it_pre) << "\t" << (*uid_it_aft) << "\n";
						fileOutWithTime << (*uid_it_pre) << "\t" << (*uid_it_aft) << "\t" << timeObj << "\n";

						uid_it_pre = uid_it_aft;
						uid_it_aft++;
					}
				}
			}
			catch (std::exception e) {
				e.what();
				return false;
			}
		}
		fileInput.close();
		fileOutNoTime.close();
		fileOutWithTime.close();
	}
	return true;
}
/*
 *	check vector<string> uid
 *	if (uid.size() == 0) continue.
 */
bool WeiboDataWash::extractUidRtUidFromString(const std::string &source, std::vector<std::string> &uid, std::string &rtuid)
{
	const std::string search_uid = "|#|uid:";		//7
	const std::string search_rtuid = "|#|rtUid:";	//9
	const std::string search_jing = "|#|";
	const std::string search_line = "|";
	const std::string search_table = "\t";
	const std::string search_mon = "$";
	const std::string search_number = "1234567890";

	int now_pos = 0;
 	size_t uid_record = source.find(search_uid.c_str(), 0);
	if (uid_record == std::string::npos)
		return false;
	//"|#|uid:" locate after the :
	uid_record += 7;

	//locate to the next |#|, extract all the uid between
	size_t next_jing = source.find_first_of(search_jing.c_str(), uid_record);

	//extract all the uid , save to set
	while (uid_record < next_jing) {
		size_t uid_next;
		
		//find the first letter which is not number , judge whether it is | or \t  , if  $ drop
		if ((uid_next = source.find_first_not_of(search_number, uid_record)) == std::string::npos) {
			//std::cerr << "find first not of error, string : " << source << std::endl;
			return false;
		}

		//extract the substring of uid, save to set 
		std::string temp_uid = source.substr(uid_record, uid_next - uid_record);
		auto iter = uid.begin();
		while (iter != uid.end() && (*iter) != temp_uid)
			iter++;
		if (iter == uid.end())
			uid.push_back(temp_uid);

		char temp[2];
		temp[1] = '\0';
		temp[0] = source[uid_next];

		//if it is | , means uid is all over
		if (strcmp(temp, search_line.c_str()) == 0)
			break;
		if (strcmp(temp, search_mon.c_str()) == 0)
			break;
		// \t  continue
		if (strcmp(temp, search_table.c_str()) == 0 || strcmp(temp, " ") == 0) {
			uid_record = uid_next + 1;
		}
	}
	//extract rtuid
	size_t rtuid_record = source.find(search_rtuid, uid_record);

	//not find, drop
	if (rtuid_record == std::string::npos) {
		rtuid.clear();
		//std::cerr << "this string has no rtuid : " << source << std::endl;
		return false;
	}
	// "|#|rtUid:"
	rtuid_record += 9;

	//find the next boundary |
	size_t rtuid_next = source.find_first_not_of(search_number, rtuid_record);
	std::string rtuid_temp = source.substr(rtuid_record, rtuid_next - rtuid_record);
	rtuid = rtuid_temp;
	
	return true;
}
/*
 *	use the rtTime as the re-tweet time.
 */
bool WeiboDataWash::extractTimeFromString(const std::string &source, WeiboTime &weiboTime)
{
	const std::string search_time = "|#|rtTime:";	//10
	size_t time_record = source.find(search_time.c_str(), 0);
	if (time_record == std::string::npos)
		return false;
	
	//std::cout << "rtTime: at " << time_record << std::endl;	/////
	time_record += 10;

	const std::string search_mid = "|#|rtMid:";	 //8
	size_t mid_record = source.find(search_mid.c_str(), 0);
	if (mid_record == std::string::npos)
		return false;

	//std::cout << search_mid << " at " << mid_record << std::endl;//////////////	
	std::string time = source.substr(time_record, mid_record - time_record);

	if (time.size() != 19) {
		//std::cerr << "size not correct, time = " << time << std::endl;
		return false;
	}
	if (time.substr(0, 4) == "00" || time.substr(5, 2) == "00" || time.substr(8, 2) == "00") {
		//std::cerr << "year | month | day == 0, time = " << time << std::endl;
		return false;
	}
	std::vector<unsigned> timeVec;
	timeVec.push_back(atoi(time.substr(0, 4).c_str()));
	timeVec.push_back(atoi(time.substr(5, 2).c_str()));
	timeVec.push_back(atoi(time.substr(8, 2).c_str()));
	timeVec.push_back(atoi(time.substr(11, 2).c_str()));
	timeVec.push_back(atoi(time.substr(14, 2).c_str()));
	timeVec.push_back(atoi(time.substr(17, 2).c_str()));
	
	if (weiboTime.setContri(timeVec) == false)
		return false;
	return true;
}

/*
 *	step two :
 *	according to the user occurred in all of the Events
 *	filter the origin user files , save what we want
 *	from ./data_origin/user_origin/
 *	to	./data_washed/user_occurred/
 */
bool WeiboDataWash::runStepTwo()
{
	if (filterTheOriginUserAccordEvent(BasicPath::EventExtractPrefix, BasicPath::OriginUserFilePrefix, BasicPath::UserOccurredPrefix) == false) {
		std::cerr << "WeiboWash::stepTwo() error." << std::endl;
		return false;
	}
	return true;
}
bool WeiboDataWash::filterTheOriginUserAccordEvent(const std::string &sourceEventPrefix, const std::string &sourceUserPrefix, const std::string &targetUserPrefix)
{
	std::multiset<std::string> setUsers;
	//std::fstream::sync_with_stdio(false);

	int fileCounter = 1;
	for (const auto &i : BasicPath::vecEventOriginFiles) {
		std::cout << "processing " << fileCounter++  << " file : " << i << std::endl;

		std::fstream inputFile(sourceEventPrefix + i, std::ios_base::in);
		if (!inputFile.is_open())
			return false;
		std::string userOne, userTwo;

		while (inputFile >> userOne >> userTwo) {
			if (whetherExistInSet(setUsers, userOne) == false)
				setUsers.insert(userOne);
			if (whetherExistInSet(setUsers, userTwo) == false)
				setUsers.insert(userTwo);
		}
		inputFile.close();
	}

	std::cout << "read finish.." << std::endl;
	std::cout << "set.size() = " << setUsers.size() << std::endl;
	getchar();

	int fileNode = 1000;
	std::fstream fiterInputFile;
	std::fstream fiterOutputFile;

	fiterOutputFile.open(targetUserPrefix + convertIntToString(fileNode), std::ios_base::out);
	if (!fiterOutputFile.is_open())
		return false;
	std::cout << "open file : " << targetUserPrefix + convertIntToString(fileNode) << std::endl;

	long long lineCounter = 0;

	for (const auto &j : BasicPath::originUserFiles) {
		std::cout << "processing: " << sourceUserPrefix + BasicPath::originUserPrefix + j << std::endl;

		fiterInputFile.open(sourceUserPrefix + originUserPrefix + j, std::ios_base::in);
		if (!fiterInputFile.is_open())
			return false;
		
		std::string tempOne, tempTwo;
		while (fiterInputFile >> tempOne >>tempTwo) {
			if (whetherExistInSet(setUsers, tempOne) && whetherExistInSet(setUsers, tempTwo)) {
				fiterOutputFile << tempOne << "\t" << tempTwo << "\n";
				lineCounter++;
			}

			if (lineCounter % 1000000 == 0)
				std::cout << "write line : " << lineCounter << std::endl;

			if (lineCounter == 3000000)
			{
				fileNode++;
				lineCounter = 0;
				fiterOutputFile.close();

				fiterOutputFile.open(targetUserPrefix + convertIntToString(fileNode), std::ios_base::out);
				if (!fiterOutputFile.is_open())
					return false;
				std::cout << "open file:" << targetUserPrefix + convertIntToString(fileNode) << std::endl;
			}

		}
		fiterInputFile.close();
	}

	if (fiterOutputFile.is_open())
		fiterOutputFile.close();
	
	return true;
}
/*
 *	if string is not in set , return true
 */
bool WeiboDataWash::whetherExistInSet(std::multiset<std::string> &users, const std::string &name)
{
	auto iter = users.begin();
	if ((iter = users.find(name)) == users.end())
		return false;
	else
		return true;
}
std::string WeiboDataWash::convertIntToString(const int u)
{
	std::stringstream ssObj;
	ssObj << u;
	std::string result = ssObj.str();
	return result;
}

/*
 *	step three:
 *	1:	read Event, build map, rehash the user string, obtain the new Event
 *		from	./data_washed/event_extract_with_time
 *		to		./data_washed/event_extract_rehashed/ && ./data_washed/event_extract_rehashed_with_time/
 *
 *	2:	according to the map we got, rehash the user string, obtain new User string.
 *		from	./data_washed/user_occurred/
 *		to		./data_washed/user_occurred_rehashed/
 *
 *	3:	delete the repeat edge for each event and all users
 *		from	./data_washed/event_extract_rehashed/ && ./data_washed/event_extract_rehashed_with_time/
 *		to		./data_washed/event_del_repeat/ && ./data_washed/event_del_repeat_time
 *
 *		from	./data_washed/user_occurred_rehashed/
 *		to		./data_washed/user_occurred_rehashed_del_rep/
 */
bool WeiboDataWash::runStepThree()
{
	std::map<std::string, unsigned> userMap;
	if (readUserMapRehashEvent(userMap, EventExtractWithTimePrefix, EventExtractRehashPrefix, EventExtractRehashTimePrefix) == false)
		return false;
	if (accordUserMapWashRehashUser(userMap, UserOccurredPrefix, UserOccurredRehashPrefix) == false)
		return false;
	
	if (delRepeatEventEdge(EventExtractRehashTimePrefix, EventExtractRehashDelRepPre, EventExtractRehashDelRepTimePre) == false)
		return false;
	if (delRepeatUserEdge(UserOccurredRehashPrefix, UserOccurredReDelRepPre) == false)
		return false;

	return true;
}
bool WeiboDataWash::readUserMapRehashEvent(std::map<std::string, unsigned> &userMap, const std::string &srcEventTimePrefix
											,const std::string &tarEventPrefix, const std::string &tarEventTimePrefix)
{
	std::cout << "WeiboDataWash::readUserMapRehashEvent() begin.." << std::endl;		//////////////////////////////

	unsigned hashValue = 1;
	
	// read event, build map
	for (const auto &file : vecEventOriginFiles) {
		std::fstream inputFile;
		std::fstream outputFile, outputTimeFile;

		inputFile.open(srcEventTimePrefix + file, std::ios_base::in);
		outputFile.open(tarEventPrefix + file, std::ios_base::out), outputTimeFile.open(tarEventTimePrefix + file, std::ios_base::out);

		if (!inputFile.is_open() || !outputFile.is_open() || !outputTimeFile.is_open()) {
			std::cerr << "WeiboWash::readUserRehash() open file error." << std::endl;
			return false;
		}
	
		std::cout << "file: " << srcEventTimePrefix + file << std::endl;	/////////////////////////////

		std::string userOne, userTwo, year, month, day, hour, minute, second;
		while (inputFile >> userOne >> userTwo >> year >> month >> day >> hour >> minute >> second) {

			unsigned valueOne = 0, valueTwo = 0;

			if (whetherExistInMap(userMap, userOne, valueOne) == false) {
				userMap.insert(make_pair(userOne, hashValue));
				valueOne = hashValue;
				hashValue++;
			}
			if (whetherExistInMap(userMap, userTwo, valueTwo) == false) {
				userMap.insert(make_pair(userTwo, hashValue));
				valueTwo = hashValue;
				hashValue++;
			}
			outputFile << valueOne << "\t" << valueTwo << "\n";
			outputTimeFile << valueOne << "\t" << valueTwo << "\t" << year << "\t" << month << "\t" << day << "\t"
							<< hour << "\t" << minute << "\t" << second << "\n";
		}
		inputFile.close(), outputFile.close(), outputTimeFile.close();
	}

	return true;
}
bool WeiboDataWash::accordUserMapWashRehashUser(std::map<std::string, unsigned> &userMap, const std::string &srcUserPrefix, const std::string &tarUserPrefix)
{
	std::cout << "WeiboDataWash::accordUserMapWashRehashUser() begin.." << std::endl;	//////////////////

	// file name
	for (int file = 1000; file <= 1008; ++file)
	{
		std::string fileName = convertIntToString(file);
		std::fstream inputFile, outputFile;
		inputFile.open(srcUserPrefix + fileName, std::ios_base::in), outputFile.open(tarUserPrefix + fileName, std::ios_base::out);
		if (!inputFile.is_open() || !outputFile.is_open()) {
			std::cerr << "WeiboWash::accord() open file error." << std::endl;
			return false;
		}

		std::string userOne, userTwo;
		while (inputFile >> userOne >> userTwo) {
			unsigned valueOne, valueTwo;
			if (whetherExistInMap(userMap, userOne, valueOne) == false) {
				std::cerr << "WeiboWash::accord(): could not find " << userOne << std::endl;
				getchar();
				return false;
			}
			if (whetherExistInMap(userMap, userTwo, valueTwo) == false) {
				std::cerr << "WeiboWash::accord(): could not find " << userTwo<< std::endl;
				getchar();
				return false;
			}
			outputFile << valueOne << "\t" << valueTwo << "\n";
		}
		inputFile.close(), outputFile.close();
	}

	return true;
}
bool WeiboDataWash::whetherExistInMap(std::map<std::string, unsigned> &userMap, const std::string &key, unsigned &u)
{
	auto iter = userMap.begin();
	if ((iter = userMap.find(key)) == userMap.end())
		return false;

	u = iter->second;
	return true;
}

bool WeiboDataWash::delRepeatEventEdge(const std::string &srcEveRehashTimePre, const std::string &tarEveDelPre, const std::string &tarEveDelTimePre)
{
	std::cout << "Weibo::delReEveEdge(): begin.." << std::endl;

	for (const auto &file : vecEventOriginFiles) {
		std::fstream inputFile;
		std::fstream outputFileDel, outputFileDelTime;
		inputFile.open(srcEveRehashTimePre + file, std::ios_base::in);
		outputFileDel.open(tarEveDelPre + file, std::ios_base::out), outputFileDelTime.open(tarEveDelTimePre + file, std::ios_base::out);
		if (!inputFile.is_open() || !outputFileDel.is_open() || !outputFileDelTime.is_open()) {
			std::cerr << "Weibo:delReEveEdge() open file error." << std::endl;
			return false;
		}
		std::cout << "process: " << srcEveRehashTimePre + file << std::endl;	/////////////

		std::multiset<std::pair<unsigned, unsigned>> collectSet;
		unsigned userOne, userTwo, year, month, day, hour, minute, second;
		while (inputFile >> userOne >> userTwo >> year >> month >> day >> hour >> minute >> second) {
			auto iter = collectSet.find(std::make_pair(userOne, userTwo));
			if (iter == collectSet.end()) {
				collectSet.insert(std::make_pair(userOne, userTwo));

				outputFileDel << userOne << "\t" << userTwo << "\n";
				outputFileDelTime << userOne << "\t" << userTwo << "\t" << year << "\t" << month << "\t" << day
					<< "\t" << hour << "\t" << minute << "\t" << second << "\n";
			}
			else
				continue;
		}
		inputFile.close(), outputFileDel.close(), outputFileDelTime.close();
	}
	return true;
}
bool WeiboDataWash::delRepeatUserEdge(const std::string &srcUserRehashPre, const std::string &tarUserRehashDelPre)
{
	std::cout << "Weibo::delRepUserEdge() begin.." << std::endl;

	for (int file = 1000; file <= 1008; ++file) {
		std::fstream inputFile, outputFile;
		inputFile.open(srcUserRehashPre + convertIntToString(file), std::ios_base::in);
		outputFile.open(tarUserRehashDelPre + convertIntToString(file), std::ios_base::out);
		if (!inputFile.is_open() || !outputFile.is_open()) {
			std::cerr << "Weibo::delRepUser() open file error." << std::endl;
			return false;
		}
		std::cout << "process: " << tarUserRehashDelPre + convertIntToString(file) << std::endl;	//////////////////
		
		std::multiset<std::pair<unsigned, unsigned>> collectSet;
		unsigned userOne, userTwo;
		while (inputFile >> userOne >> userTwo) {
			auto iter = collectSet.find(std::make_pair(userOne, userTwo));
			if (iter == collectSet.end()) {
				collectSet.insert(std::make_pair(userOne, userTwo));

				outputFile << userOne << "\t" << userTwo << "\n";
			}
			else
				continue;
		}

		inputFile.close(), outputFile.close();
	}
	return true;
}
