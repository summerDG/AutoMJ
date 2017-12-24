/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pasalab.automj;

import org.gnu.glpk.*;

/**
 * The factory class is used to generate exponents by LP using GLPK. Completed on 16/10/30.
 * By xiaoqi wu.
 */
public class GenerateShares {
    //	Minimize lamda
    //
    //	subject to
    //	e1 + e2 + ... + en <= 1
    //	For any j, Sigma(ei: i E Sj) + lamda >= uj
    //	where,
    //	0.0 <= ei
    //	0.0 <= lamda

//    public static void main(String[] arg) {
//        int tables[][] = {{1,2},{2,3},{3,1}};
//        double sizes[] = {10,10,10};
//        int n = 24;
//        int dims = 3;
//        double[] ps = generateLp(sizes, tables, n, dims);
//        for (double p : ps) {
//            System.out.println(p);
//        }
//    }

    public static double[] generateLp(double[] sizes, int[][] tables, int n, int dims) {
        glp_prob lp;
        glp_smcp parm;
        SWIGTYPE_p_int ind;
        SWIGTYPE_p_double val;
        double[] result = new double[dims];
        int ret;

        try {
            // Create problem
            lp = GLPK.glp_create_prob();
            GLPK.glp_set_prob_name(lp, "Generate Non-Integer shares");
            // Define columns
            GLPK.glp_add_cols(lp, dims + 2);

            for (int i = 1; i <= dims; i++) {
                GLPK.glp_set_col_name(lp, i, "e" + i);
                GLPK.glp_set_col_kind(lp, i, GLPKConstants.GLP_CV);
                GLPK.glp_set_col_bnds(lp, i, GLPKConstants.GLP_DB, 0., 1.);
            }
            GLPK.glp_set_col_name(lp, dims + 1, "lamda");
            GLPK.glp_set_col_kind(lp, dims + 1, GLPKConstants.GLP_CV);
            GLPK.glp_set_col_bnds(lp, dims + 1, GLPKConstants.GLP_LO, 0., 1);

            // Allocate memory
            ind = GLPK.new_intArray(dims + 2);
            val = GLPK.new_doubleArray(dims + 2);
            // Create rows
            GLPK.glp_add_rows(lp, 1 + sizes.length);

            GLPK.glp_set_row_name(lp, 1, "c1");
            GLPK.glp_set_row_bnds(lp, 1, GLPKConstants.GLP_UP, 0, 1.);
            for (int i = 1; i <= dims; i++) {
                GLPK.intArray_setitem(ind, i, i);
                GLPK.doubleArray_setitem(val, i, 1.);
            }
            GLPK.glp_set_mat_row(lp, 1, dims, ind, val);

            // For any j
            for (int  j = 0; j < sizes.length; j++) {
                int num = j + 2;
                GLPK.glp_set_row_name(lp, num, "c" + num);
                GLPK.glp_set_row_bnds(lp, num, GLPKConstants.GLP_LO, sizes[j], 1);
                for (int i = 0; i < tables[j].length; i++) {
                    GLPK.intArray_setitem(ind, i + 1, tables[j][i]);
                    GLPK.doubleArray_setitem(val, i + 1, 1.);
                }

                GLPK.intArray_setitem(ind, tables[j].length + 1, dims + 1);
                GLPK.doubleArray_setitem(val, tables[j].length + 1, 1.0);
                GLPK.glp_set_mat_row(lp, num, tables[j].length + 1, ind, val);
            }

            // free memory
            GLPK.delete_intArray(ind);
            GLPK.delete_doubleArray(val);

            // define objective
            GLPK.glp_set_obj_name(lp, "z");
            GLPK.glp_set_obj_dir(lp, GLPKConstants.GLP_MIN);
            GLPK.glp_set_obj_coef(lp, 0, 0.);
            GLPK.glp_set_obj_coef(lp, dims + 1, 1.);

            GLPK.glp_write_lp(lp, null, "shares.lp");

            // Solve model
            parm = new glp_smcp();
            GLPK.glp_init_smcp(parm);
            ret = GLPK.glp_simplex(lp, parm);


            // Retrieve solution
            if (ret == 0) {
                double[] e = extract_lp_solution(lp);
                for (int i = 0; i < dims; i++) {
                    result[i] = Math.pow(n, e[i]);
                }
            } else {
                throw new RuntimeException("The linear programing can not be solved.");
            }

            GLPK.glp_delete_prob(lp);
        } catch (GlpkException ex) {
            ret = 1;
            ex.printStackTrace();
        }
        return result;
    }
    /**
     * extract simplex solution
     * @param lp problem
     */
    static double[] extract_lp_solution(glp_prob lp) {
        int n = GLPK.glp_get_num_cols(lp);
        double[] result = new double[n];
        for (int i = 1; i <= n; i++) {
            result[i-1] = GLPK.glp_get_col_prim(lp, i);
        }
        return result;
    }
}
