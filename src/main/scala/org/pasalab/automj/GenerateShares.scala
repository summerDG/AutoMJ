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

import org.gnu.glpk._;

/**
 * The factory class is used to generate exponents by LP using GLPK. Completed on 16/10/30.
 * By xiaoqi wu.
 */
object GenerateShares {
    //	Minimize lamda
    //
    //	subject to
    //	e1 + e2 + ... + en <= 1
    //	For any j, Sigma(ei: i E Sj) + lamda >= uj
    //	where,
    //	0.0 <= ei
    //	0.0 <= lamda

    def generateLp(sizes: Array[Double], tables: Array[Array[Int]], n: Int, dims: Int): Array[Double] = {
        var lp: glp_prob = null
        var parm: glp_smcp = null
        var ind: SWIGTYPE_p_int = null
        var value: SWIGTYPE_p_double = null
        val result:Array[Double] = new Array[Double](dims)
        var ret: Int = 0

        try {
            // Create problem
            lp = GLPK.glp_create_prob()
            GLPK.glp_set_prob_name(lp, "Generate Non-Integer shares")
            // Define columns
            GLPK.glp_add_cols(lp, dims + 2)

            for (i <- 1 to dims) {
                GLPK.glp_set_col_name(lp, i, "e" + i)
                GLPK.glp_set_col_kind(lp, i, GLPKConstants.GLP_CV)
                GLPK.glp_set_col_bnds(lp, i, GLPKConstants.GLP_DB, 0., 1.)
            }
            GLPK.glp_set_col_name(lp, dims + 1, "lamda")
            GLPK.glp_set_col_kind(lp, dims + 1, GLPKConstants.GLP_CV)
            GLPK.glp_set_col_bnds(lp, dims + 1, GLPKConstants.GLP_LO, 0., 1)

            // Allocate memory
            ind = GLPK.new_intArray(dims + 2)
            value = GLPK.new_doubleArray(dims + 2)
            // Create rows
            GLPK.glp_add_rows(lp, 1 + sizes.length)

            GLPK.glp_set_row_name(lp, 1, "c1")
            GLPK.glp_set_row_bnds(lp, 1, GLPKConstants.GLP_UP, 0, 1.)
            for (i <- 1 to dims) {
                GLPK.intArray_setitem(ind, i, i)
                GLPK.doubleArray_setitem(value, i, 1.)
            }
            GLPK.glp_set_mat_row(lp, 1, dims, ind, value)

            // For any j
            for (j <- 0 to sizes.length - 1) {
                val num = j + 2
                GLPK.glp_set_row_name(lp, num, "c" + num)
                GLPK.glp_set_row_bnds(lp, num, GLPKConstants.GLP_LO, sizes(j), 1)
                for (i <- 0 to tables(j).length) {
                    GLPK.intArray_setitem(ind, i + 1, tables(j)(i))
                    GLPK.doubleArray_setitem(value, i + 1, 1.)
                }

                GLPK.intArray_setitem(ind, tables(j).length + 1, dims + 1)
                GLPK.doubleArray_setitem(value, tables(j).length + 1, 1.0)
                GLPK.glp_set_mat_row(lp, num, tables(j).length + 1, ind, value)
            }

            // free memory
            GLPK.delete_intArray(ind)
            GLPK.delete_doubleArray(value)

            // define objective
            GLPK.glp_set_obj_name(lp, "z")
            GLPK.glp_set_obj_dir(lp, GLPKConstants.GLP_MIN)
            GLPK.glp_set_obj_coef(lp, 0, 0.)
            GLPK.glp_set_obj_coef(lp, dims + 1, 1.)

            GLPK.glp_write_lp(lp, null, "shares.lp")

            // Solve model
            parm = new glp_smcp()
            GLPK.glp_init_smcp(parm)
            ret = GLPK.glp_simplex(lp, parm)


            // Retrieve solution
            if (ret == 0) {
                val e = extract_lp_solution(lp)
                for (i <- 0 to dims - 1) {
                    result(i) = Math.pow(n, e(i))
                }
            } else {
                throw new RuntimeException("The linear programing can not be solved.")
            }

            GLPK.glp_delete_prob(lp);
        } catch {
          case ex: GlpkException =>
            ret = 1
            ex.printStackTrace()
        }
        return result
    }
    /**
     * extract simplex solution
     * @param lp problem
     */
    def extract_lp_solution(lp: glp_prob) :Array[Double] = {
        val n = GLPK.glp_get_num_cols(lp)
        val result = new Array[Double](n)
        for (i <- 1 to n) {
            result(i - 1) = GLPK.glp_get_col_prim(lp, i)
        }
        return result
    }
}
