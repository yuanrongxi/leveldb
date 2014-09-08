#ifndef __LEVEL_DB_HISTOGRAM_H_
#define __LEVEL_DB_HISTOGRAM_H_

#include <string>

namespace leveldb{

class Histogram
{
public:
	Histogram() {};
	~Histogram() {};

	void Clear();
	void Add(double value);
	void Merge(const Histogram& other);

	std::string ToString() const;

private:
	double Median() const;
	double Percentile(double p) const;
	double Average() const;
	double StandardDeviation() const;

private:
	double min_;
	double max_;
	double num_;
	double sum_;
	double sum_squares_;

	enum{ kNumBuckets = 154 };
	static const double kBucketLimit[kNumBuckets];
	double buckets_[kNumBuckets];
};
};

#endif
