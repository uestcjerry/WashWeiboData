#ifndef WASH_WEIBO_DATA_H
#define WASH_WEIBO_DATA_H

#include <iostream>
#include <vector>
#include <set>
#include <map>
//#include <initializer_list>


namespace BasicPath {
	const std::string FilePrefix = "e:\\data_of_weibo\\";

	// step one
	const std::string OriginEventFilePrefix = FilePrefix + "data_origin\\event_origin\\";
	const std::string OriginUserFilePrefix = FilePrefix + "data_origin\\user_origin\\";

	const std::string EventExtractPrefix = FilePrefix + "data_washed\\event_extract\\";
	const std::string EventExtractWithTimePrefix = FilePrefix + "data_washed\\event_extract_with_time\\";
	
	const std::string UserOccurredPrefix = FilePrefix + "data_washed\\user_occurred\\";

	// step two
	const std::string EventExtractRehashPrefix = FilePrefix + "data_washed\\event_extract_rehashed\\";
	const std::string EventExtractRehashTimePrefix = FilePrefix + "data_washed\\event_extract_rehashed_with_time\\";
	
	const std::string UserOccurredRehashPrefix = FilePrefix + "data_washed\\user_occurred_rehashed\\";
	
	// step three
	const std::string EventExtractRehashDelRepPre = FilePrefix + "data_washed\\event_del_repeat\\";
	const std::string EventExtractRehashDelRepTimePre = FilePrefix + "data_washed\\event_del_repeat_time\\";
	const std::string UserOccurredReDelRepPre = FilePrefix + "data_washed\\user_occurred_rehashed_del_rep\\";

	// analysis
	const std::string calUserOverlapCoeffiBetDifEvePre = FilePrefix + "data_washed\\analysis_result\\";


	// base
	const std::string originUserPrefix = "part-000";
	const std::vector<std::string> originUserFiles = {
		"00", "01", "02", "03", "04", "05",
		"06", "07", "08", "10", "11", "12",
		"13", "15", "16", "18", "19", "20",
		"21", "22", "23", "25", "26", "28",
		"29"
	};

	const std::vector<std::string> vecEventOriginFiles = {
		"Anshun incident",
		"Bohai bay oil spill",
		"case of running fast car in Heibei University",
		"Chaozhou riot",
		"China Petro chemical Co. Ltd",
		"Chongqing gang trials",
		"death of Muammar Gaddafi",
		"death of Steve Jobs",
		"Death of Wang Yue",
		"Deng Yujiao incident",
		"earthquake of Yunnan Yingjiang",
		"family violence of Li Yang",
		"Foxconn worker falls to death",
		"Fuzhou bombings",
		"Gansu school bus crash",
		"Guo Meimei",
		"House prices",
		"incident of self-burning at Yancheng, Jangsu",
		"individual income tax threshold rise up to 3500",
		"iphone4s release",
		"Japan Earthquake",
		"Li Na win French Open in tennis",
		"line 10 of Shanghai-Metro pileup",
		"mass suicide at Nanchang Bridge",
		"Motorola was acquisitions by Google",
		"protests of Wukan",
		"Qian Yunhui",
		"Qianxi riot",
		"Shanghai government's urban management officers attack migrant workers in 2011",
		"Shanxi",
		"Shenzhou-8 launch successfully",
		"Spain Series A League",
		"Tang Jun educatioin qualification fake",
		"the death of Kim Jongil",
		"the death of Osama Bin Laden",
		"Tiangong-1 launch successfully",
		"Wenzhou train collision",
		"Windows Phone release",
		"Xiaomi release",
		"Yao Jiaxin murder case",
		"Yao Ming retire",
		"Yihuang self-immolation incident",
		"Yushu earthquake",
		"Zhili disobey tax official violent",
		"Zhouqu landslide"
	};

	const std::vector<std::string> vecBigEventFiles = {
		"House prices",
		"Guo Meimei",
		"death of Muammar Gaddafi",
		"the death of Osama Bin Laden",
		"case of running fast car in Heibei University",
		"Wenzhou train collision",
		"Tang Jun educatioin qualification fake",
		"Xiaomi release",
		"the death of Kim Jongil",
		"family violence of Li Yang",
		"Qian Yunhui",
		"China Petro chemical Co. Ltd",
		"Death of Wang Yue",
		"Yushu earthquake",
		"Yao Ming retire",
		"earthquake of Yunnan Yingjiang",
		"Windows Phone release",
		"Chongqing gang trials",
		"individual income tax threshold rise up to 3500",
		"incident of self-burning at Yancheng, Jangsu",
		"Foxconn worker falls to death"
	};

};

class WeiboTime {
public:
	friend bool operator<(const WeiboTime &, const WeiboTime &);
	friend bool operator==(const WeiboTime &, const WeiboTime &);
	friend std::ostream& operator<<(std::ostream &, const WeiboTime &);

	WeiboTime();
	WeiboTime(unsigned, unsigned, unsigned, unsigned, unsigned, unsigned);
	WeiboTime(const WeiboTime &);
	WeiboTime& operator=(const WeiboTime &);

	bool setContri(std::vector<unsigned> &);

private:
	unsigned int year;
	unsigned int month;
	unsigned int day;

	unsigned int hour;				// string may be "00"
	unsigned int minute;
	unsigned int second;
};

class WeiboDataWash {
public:

	// step one
	bool runStepOne();

	bool processOriginEventExtractEdge(const std::string &, const std::string &, const std::string &);
	bool extractUidRtUidFromString(const std::string &, std::vector<std::string> &, std::string &);
	bool extractTimeFromString(const std::string &, WeiboTime &);

	// step two
	bool runStepTwo();
	
	bool filterTheOriginUserAccordEvent(const std::string &, const std::string &, const std::string &);
	bool whetherExistInSet(std::multiset<std::string> &, const std::string &);
	std::string convertIntToString(const int);

	// step three
	bool runStepThree();

	bool readUserMapRehashEvent(std::map<std::string, unsigned> &, const std::string &, const std::string &, const std::string &);
	bool accordUserMapWashRehashUser(std::map<std::string, unsigned> &, const std::string &, const std::string &);
	bool whetherExistInMap(std::map<std::string, unsigned> &, const std::string &, unsigned &);

	bool delRepeatEventEdge(const std::string &, const std::string &, const std::string &);
	bool delRepeatUserEdge(const std::string &, const std::string &);


	// network test
	// max user = 1767200
	bool testEdge();

	bool analysisUserOverlap();
	bool calUserOverCoeffiBetDifEvents(const std::string &, const std::string &);
	template<typename T> bool isInSet(std::set<T> &, const T);
	template<typename T> double calculateSet(std::set<T> &, std::set<T> &);

};



#endif