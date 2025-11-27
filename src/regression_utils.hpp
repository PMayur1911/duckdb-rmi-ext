#pragma once
#include <vector>
#include <utility>
#include <cmath>
#include <cstddef>

using std::vector;
using std::pair;

pair<double,double> FitSimpleLinear(const vector<double> &x,
                                    const vector<double> &y);

vector<double> FitBestPolynomial(const vector<double> &x,
                                 const vector<double> &y,
                                 int max_degree = 10);

double EvalPolynomial(const vector<double> &coeffs, double x);

size_t ClampIndex(double value, size_t n);
