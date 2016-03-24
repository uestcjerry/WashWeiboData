#include "Wash_weibo_data.h"
#include <string>

using namespace std;

int main(int argc, char* argv[])
{
	WeiboDataWash obj;

	//if (obj.runStepOne() == false)
	//	return false;

	//if (obj.runStepTwo() == false)
	//	return false;

	if (obj.runStepThree() == false)
		return false;
	

	return 0;
}