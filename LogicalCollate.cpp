/*
 *    _____      _ ____  ____
 *   / ___/_____(_) __ \/ __ )
 *   \__ \/ ___/ / / / / __  |
 *  ___/ / /__/ / /_/ / /_/ / 
 * /____/\___/_/_____/_____/  
 *
 *
 * BEGIN_COPYRIGHT
 *
 * This file is part of SciDB.
 * Copyright (C) 2008-2014 SciDB, Inc.
 *
 * SciDB is free software: you can redistribute it and/or modify
 * it under the terms of the AFFERO GNU General Public License as published by
 * the Free Software Foundation.
 *
 * SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
 * INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
 * NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
 * the AFFERO GNU General Public License for the complete license terms.
 *
 * You should have received a copy of the AFFERO GNU General Public License
 * along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
 *
 * END_COPYRIGHT
 */

/**
 * @file LogicalCollate.cpp
 * A sample UDO that converts a 1-d array with one or more uniformly-typed
 * attributes into a 2-d array with a single attribute (a matrix).  The columns
 * of the output matrix correspond to the attributes in the input array.
 *
 * @brief The operator: collate(A).
 *
 * @par Synopsis: collate(A).
 *  
 * @par Summary:
 *   <br>
 *   Convert a 1-d array with one or more uniformly typed attributes into a
 *   2-d array with a single attribute whose columns correspond to the input
 *   array attributes.
 *
 * @par Input:
 *   - A : <a_1, a_2, ..., a_n>[i=0:*, chunksize, 0] (The input 1-d array with n attributes)
 *
 * @par Output array:
 *   <br> <
 *   <br>   val
 *   <br> >
 *   <br> [
 *   <br>   i=0:*, chunksize, 0,
 *   <br>   j=0:n, n+1, 0
 *   <br> ]
 *
 * @par Examples:
 * <br> load_library('collate')
 * <br> collate(apply(build(<v:double>[i=0:9,3,0],i),w,i+0.5))
 *
 * @author apoliakov@paradigm4.com, blewis@paradigm4.com
 */

#include "query/Operator.h"

namespace scidb
{

class LogicalCollate : public LogicalOperator
{
public:
    LogicalCollate(const string& logicalName, const string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT()
        _usage = "collate(A)\n"
                 "where:\n"
                 "A is a 1-d matrix with one or more uniformly-typed attributes.\n\n"
                 "collate(A) returns a 2-d array that copies the attributes of A into\n"
                 "columns of an output matrix.\n\n"
                 "Note: The output matrix row dimension will have a chunk size equal\n"
                 "to the input array, and column chunk size equal to the number of columns.\n\n"
                 "EXAMPLE:\n\n"
                 "collate(apply(build(<v:double>[i=0:9,3,0],i),w,i+0.5))";
    }

// Relax this to simply check that they are all of the same type
    void checkInputAttributes(ArrayDesc const& inputSchema)
    {
        Attributes const& attrs = inputSchema.getAttributes(true);
        size_t const nAttrs = attrs.size();
        bool ok = true;
        for (AttributeID i = 1; i<nAttrs; ++i)
        {
            ok = ok && attrs[i].getType() == attrs[0].getType();
        }
        if(!ok)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ILLEGAL_OPERATION)
                  << "collate requires that all input array attributes have the same type";
        }
    }

    ArrayDesc inferSchema(vector< ArrayDesc> schemas, shared_ptr< Query> query)
    {
        ArrayDesc const& inputSchema = schemas[0];
        checkInputAttributes(inputSchema);
        Attributes const& attrs = inputSchema.getAttributes(true);
        size_t nAttrs  = attrs.size();
        Attributes outputAttributes;
        outputAttributes.push_back(AttributeDesc(0, "val", attrs[0].getType(), AttributeDesc::IS_NULLABLE, 0));
        outputAttributes = addEmptyTagAttribute(outputAttributes);
        Dimensions outputDimensions;
        outputDimensions.push_back(DimensionDesc("i", 0, inputSchema.getDimensions()[0].getEndMax(), inputSchema.getDimensions()[0].getChunkInterval(), 0));
        outputDimensions.push_back(DimensionDesc("j", 0, nAttrs-1, nAttrs, 0));
        return ArrayDesc(inputSchema.getName(), outputAttributes, outputDimensions);

    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalCollate, "collate");

}
