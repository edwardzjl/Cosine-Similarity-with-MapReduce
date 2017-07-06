/*
 * Copyright 2014 Giannakouris - Salalidis Victor
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.gsvic.csmr.CSMRBase;

import java.io.IOException;

public class main {

    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException {


//        CSMRBase.generatePairs(args[0], args[1]);
        CSMRBase.generatePairs("input-vectors/tfidf-vectors/part-r-00000", "output");
        CSMRBase.StartCSMR();
    }

}
