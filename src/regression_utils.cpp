#include "regression_utils.hpp"
#include <limits>
#include <cmath>

// Solve Ax = b by Gaussian elimination
static bool SolveLinearSystem(vector<vector<double>> &A,
                              vector<double> &b,
                              vector<double> &x_out) {
    size_t n = A.size();
    x_out.assign(n, 0.0);

    for (size_t i = 0; i < n; i++) {
        size_t pivot = i;
        double max_abs = fabs(A[i][i]);
        for (size_t r = i + 1; r < n; r++) {
            if (fabs(A[r][i]) > max_abs) {
                pivot = r;
                max_abs = fabs(A[r][i]);
            }
        }
        if (max_abs < 1e-12) return false;

        if (pivot != i) {
            std::swap(A[i], A[pivot]);
            std::swap(b[i], b[pivot]);
        }

        double diag = A[i][i];
        for (size_t c = i; c < n; c++)
            A[i][c] /= diag;
        b[i] /= diag;

        for (size_t r = i + 1; r < n; r++) {
            double f = A[r][i];
            if (fabs(f) < 1e-12) continue;
            for (size_t c = i; c < n; c++)
                A[r][c] -= f * A[i][c];
            b[r] -= f * b[i];
        }
    }

    for (size_t i = n; i-- > 0;) {
        double v = b[i];
        for (size_t c = i + 1; c < n; c++)
            v -= A[i][c] * x_out[c];
        x_out[i] = v;
    }
    return true;
}

pair<double,double> FitSimpleLinear(const vector<double> &x,
                                    const vector<double> &y) {
    size_t n = x.size();
    if (n == 0) return {0.0, 0.0};

    double sx=0, sy=0, sxy=0, sx2=0;
    for (size_t i = 0; i < n; i++) {
        sx += x[i];
        sy += y[i];
        sxy += x[i]*y[i];
        sx2 += x[i]*x[i];
    }

    double denom = n*sx2 - sx*sx;
    if (fabs(denom) < 1e-12)
        return {0.0, sy/double(n)};

    double a = (n*sxy - sx*sy) / denom;
    double b = (sy - a*sx) / double(n);
    return {a, b};
}

vector<double> FitBestPolynomial(const vector<double> &x,
                                 const vector<double> &y,
                                 int max_degree) {
    size_t n = x.size();
    vector<double> best = {0.0, 1.0};
    double best_mse = std::numeric_limits<double>::infinity();

    for (int d = 1; d <= max_degree; d++) {
        int m = d + 1;
        vector<vector<double>> ATA(m, vector<double>(m,0));
        vector<double> ATy(m,0);

        for (size_t i = 0; i < n; i++) {
            double xp[11];
            xp[0]=1;
            for (int k=1;k<=d;k++) xp[k]=xp[k-1]*x[i];

            for (int r=0;r<m;r++) {
                ATy[r]+=xp[r]*y[i];
                for(int c=0;c<m;c++)
                    ATA[r][c]+=xp[r]*xp[c];
            }
        }
        vector<double> coeffs;
        if(!SolveLinearSystem(ATA,ATy,coeffs)) continue;

        double sse=0;
        for(size_t i=0;i<n;i++){
            double pred=0, power=1;
            for(int k=0;k<=d;k++){
                pred += coeffs[k] * power;
                power *= x[i];
            }
            double diff = y[i]-pred;
            sse += diff*diff;
        }
        double mse = sse/double(n);
        if(mse < best_mse){
            best_mse = mse;
            best = coeffs;
        }
    }
    return best;
}

double EvalPolynomial(const vector<double> &coeffs, double x){
    double r=0;
    for(size_t i=coeffs.size(); i-- > 0; )
        r = r*x + coeffs[i];
    return r;
}

size_t ClampIndex(double v, size_t n){
    if(n==0) return 0;
    if(v < 0) return 0;
    if(v >= double(n)) return n-1;
    return (size_t)v;
}
