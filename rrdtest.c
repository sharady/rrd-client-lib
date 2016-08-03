/*
 * Copyright (c) 2016 Citrix
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE. 
 */


#include <stdio.h>
#include <stdlib.h>
#include <libgen.h>
#include <assert.h>
#include <string.h>

#include "librrd.h"
#include "parson/parson.h"


static int number = 0;
static int tests_passed = 0;
static int tests_failed = 0;
static int numbers[] = { 2, 16, 28, 34, 40, 52, 66, 71, 83, 90, 100, 111};

int get_number()
{
    static int i = 0;
    return numbers[i++ % (sizeof(numbers) / sizeof(numbers[0]))];
}

/*
 * Returns the global defined variable number which is updated by tests.
 */

static rrd_value sample()
{
    rrd_value v;
    v.int64 = (int64_t)number;
    return v;
}

RRD_SOURCE create_rrd_source(char *name, char *description, rrd_owner own, char *owner_uuid,
        char *units, rrd_scale scl, rrd_type typ, char *min, char *max, int rrd_default, rrd_value (*f)())
{
    RRD_SOURCE src;
    src.name = strdup(name);
    src.description = strdup(description);
    src.owner = own;
    src.owner_uuid = strdup(owner_uuid);
    src.rrd_units = strdup(units);
    src.scale = scl;
    src.type = typ;
    src.min = strdup(min);
    src.max = strdup(max);
    src.rrd_default = rrd_default;
    src.sample = (*f);
    return src;
}

int testRRDDataSource(char *filename, RRD_PLUGIN *plugin, int datasource_count)
{
    int file_size;
    char *buff;
    JSON_Value *plugin_json, *rrd_json;
    int skip_bytes = RRD_HEADER_SIZE + DATASOURCE_VALUE_SIZE * datasource_count + META_SIZE;

    FILE *fp = fopen(filename, "rb");
    if (!fp)
        return 0;
    
    fseek(fp, 0L, SEEK_END);
    file_size = ftell(fp);
    if (file_size < 0) {
        fclose(fp);
        return 0;
    }
    rewind(fp);

    buff = malloc(file_size - skip_bytes + 1);
    /* Skip the header and value bytes written */
    fseek(fp, skip_bytes, SEEK_SET);
    fread(buff, (file_size - skip_bytes), 1, fp);
    fclose(fp);

    /* Compare the json of plugin and DataSources metadata written in the file */
    rrd_json = json_parse_string(buff);
    plugin_json = json_for_plugin(plugin);
    return json_value_equals(plugin_json, rrd_json);
}

int testRRDValue(char *filename, int value, int datasource_count)
{
    int buffer;
    int read_byte = 1;
    int skip_bytes = RRD_HEADER_SIZE + DATASOURCE_VALUE_SIZE * datasource_count - 1;
    
    FILE *fp = fopen(filename,"rb");
    if (!fp)
        return 0;

    /* Tests are writting value within 1byte range - Only read last byte of 64bit value */
    fseek(fp, skip_bytes, SEEK_SET);
    fread(&buffer, read_byte, 1, fp);
    fclose(fp);

    if (value == (buffer & 0x00ff))
        return 1;
    
    return 0;
}

void runTests(char *filename, RRD_PLUGIN *plugin, int number, int datasource_count, char *op, char *datasource)
{
    if (testRRDValue(filename, number, datasource_count) && testRRDDataSource(filename, plugin, datasource_count))
    {
        tests_passed++;
        printf("Test passed for %s datasource:%s in plugin:%s\n", op, datasource, plugin->name);
    }
    else
    {
        tests_failed++;
        printf("Test failed for %s datasource:%s in plugin:%s\n", op, datasource, plugin->name);
    }
}

int main(int argc, char **argv)
{
    RRD_PLUGIN *plugin1, *plugin2;
    RRD_SOURCE src1[2], src2[2];
    int rc;

    if (argc != 1) {
        fprintf(stderr, "usage: %s\n", basename(argv[0]));
        exit(1);
    }

    /* Tests for plugin1 and sources */
    plugin1 = rrd_open("rrdplugin1", RRD_LOCAL_DOMAIN, "rrdplugin1.rrd");
    assert(plugin1);

    /* Test for adding datasource:RRD_SOURCE_1 to plugin:rrdplugin1 */
    src1[0] = create_rrd_source("RRD_SOURCE_1", "First RRD source", RRD_HOST, "4cc1f2e0-5405-11e6-8c2f-572fc76ac144",
            "BYTE", RRD_GAUGE, RRD_INT64, "-inf", "inf", 1, sample);
    rrd_add_src(plugin1, &src1[0]);
    number = get_number();
    rc = rrd_sample(plugin1);
    assert(rc == RRD_OK);
    runTests("rrdplugin1.rrd", plugin1, number, 1, "adding", "RRD_SOURCE_1");

    /* Update value and check new value is written to rrd file */
    number = get_number();
    rc = rrd_sample(plugin1);
    assert(rc == RRD_OK);
    runTests("rrdplugin1.rrd", plugin1, number, 1, "updating", "RRD_SOURCE_1");

    /* Test for adding datasource:RRD_SOURCE_2 to plugin:rrdplugin1 */
    src1[1] = create_rrd_source("RRD_SOURCE_2", "Second RRD source", RRD_HOST, "e8969702-5414-11e6-8cf5-47824be728c3",
            "BYTE", RRD_GAUGE, RRD_INT64, "-inf", "inf", 1, sample);
    rrd_add_src(plugin1, &src1[1]);
    number = get_number();
    rc = rrd_sample(plugin1);
    assert(rc == RRD_OK);
    runTests("rrdplugin1.rrd", plugin1, number, 2, "adding", "RRD_SOURCE_2");

    /* Update value and check new value is written to rrd file */
    number = get_number();
    rc = rrd_sample(plugin1);
    assert(rc == RRD_OK);
    runTests("rrdplugin1.rrd", plugin1, number, 2, "updating", "RRD_SOURCE_2");

    /* Test for deleting datasource:RRD_SOURCE_1 from plugin:rrdplugin1 */
    rrd_del_src(plugin1, &src1[0]);
    rc = rrd_sample(plugin1);
    assert(rc == RRD_OK);
    runTests("rrdplugin1.rrd", plugin1, number, 1, "deleting", "RRD_SOURCE_1");

    /* Update value and check new value is written to rrd file */
    number = get_number();
    rc = rrd_sample(plugin1);
    assert(rc == RRD_OK);
    runTests("rrdplugin1.rrd", plugin1, number, 1, "updating", "RRD_SOURCE_2");

    /* Test for deleting datasource:RRD_SOURCE_2 from plugin:rrdplugin1 */
    rrd_del_src(plugin1, &src1[1]);
    rc = rrd_sample(plugin1);
    assert(rc == RRD_OK);
    rc = rrd_close(plugin1);
    assert(rc == RRD_OK);

    /* Tests for plugin2 and sources */
    plugin2 = rrd_open("rrdplugin2", RRD_LOCAL_DOMAIN, "rrdplugin2.rrd");
    assert(plugin2);

    /* Test for adding datasource:RRD_SOURCE_1 to plugin:rrdplugin2 */
    src2[0] = create_rrd_source("RRD_SOURCE_1", "First RRD source", RRD_HOST, "ff12b384-96f1-4142-a9c6-21db5fedb4a1",
            "BYTE", RRD_GAUGE, RRD_INT64, "-inf", "inf", 1, sample);
    rrd_add_src(plugin2, &src2[0]);
    number = get_number();
    rc = rrd_sample(plugin2);
    assert(rc == RRD_OK);
    runTests("rrdplugin2.rrd", plugin2, number, 1, "adding", "RRD_SOURCE_1");

    /* Update value and check new value is written to rrd file */
    number = get_number();
    rc = rrd_sample(plugin1);
    assert(rc == RRD_OK);
    runTests("rrdplugin2.rrd", plugin2, number, 1, "updating", "RRD_SOURCE_1");

    /* Test for adding datasource:RRD_SOURCE_2 to plugin:rrdplugin2 */
    src2[1] = create_rrd_source("RRD_SOURCE_2", "Second RRD source", RRD_HOST, "7730f117-5817-4aee-bbcd-4079633ee04a",
            "BYTE", RRD_GAUGE, RRD_INT64, "-inf", "inf", 1, sample);
    rrd_add_src(plugin2, &src2[1]);
    number = get_number();
    rc = rrd_sample(plugin2);
    assert(rc == RRD_OK);
    runTests("rrdplugin2.rrd", plugin2, number, 2, "adding", "RRD_SOURCE_2");

    /* Update value and check new value is written to rrd file */
    number = get_number();
    rc = rrd_sample(plugin2);
    assert(rc == RRD_OK);
    runTests("rrdplugin2.rrd", plugin2, number, 2, "updating", "RRD_SOURCE_2");

    /* Test for deleting datasource:RRD_SOURCE_1 from plugin:rrdplugin2 */
    rrd_del_src(plugin2, &src2[0]);
    rc = rrd_sample(plugin2);
    assert(rc == RRD_OK);
    runTests("rrdplugin2.rrd", plugin2, number, 1, "deleting", "RRD_SOURCE_1");

    /* Update value and check new value is written to rrd file */
    number = get_number();
    rc = rrd_sample(plugin2);
    assert(rc == RRD_OK);
    runTests("rrdplugin2.rrd", plugin2, number, 1, "updating", "RRD_SOURCE_2");

    /* Test for deleting datasource:RRD_SOURCE_2 from plugin:rrdplugin2 */
    rrd_del_src(plugin2, &src2[1]);
    rc = rrd_sample(plugin2);
    assert(rc == RRD_OK);
    rc = rrd_close(plugin2);
    assert(rc == RRD_OK);

    printf("Total Tests=%d, Tests Passed=%d, Tests Failed=%d\n",(tests_passed+tests_failed), tests_passed, tests_failed);

    return 0;
}

