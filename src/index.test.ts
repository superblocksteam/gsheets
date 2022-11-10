import { GoogleSheetsFormatType } from '@superblocksteam/shared';
import { sheets_v4 } from 'googleapis';
import GoogleSheetsPlugin from '.';

describe('g-sheets read', () => {
  test("reading a spreadsheet, that has duplicate column names, don't extract a header from a spreadsheet", async () => {
    const plugin: GoogleSheetsPlugin = new GoogleSheetsPlugin();
    const mockedValues = [
      ['Name', 'stocks', 'stocks', 'stocks', 'bonds', 'bonds', 'bonds'],
      ['Subname', 'ACI', 'ALLY', 'ALV', 'BND', 'AGG', 'LQD'],
      ['Butterfly', '57', '763', '32', '320', '1357', '5305'],
      ['Lion', '23', '426', '501', '439', '5162', '1052'],
      ['Phoenix', '56', '845', '405', '615', '8614', '5006']
    ];
    const getValuesMock = jest.fn().mockReturnValue({ data: { values: mockedValues } });
    const sheetsClient = { spreadsheets: { values: { get: getValuesMock } } };
    const readResult = await plugin.readFromSpreadsheet(
      (sheetsClient as unknown) as sheets_v4.Sheets,
      'sheetId',
      'sheetTitle',
      false,
      GoogleSheetsFormatType.FORMATTED_VALUE
    );
    expect(readResult).toEqual([
      {
        column0: 'Name',
        column1: 'stocks',
        column2: 'stocks',
        column3: 'stocks',
        column4: 'bonds',
        column5: 'bonds',
        column6: 'bonds'
      },
      {
        column0: 'Subname',
        column1: 'ACI',
        column2: 'ALLY',
        column3: 'ALV',
        column4: 'BND',
        column5: 'AGG',
        column6: 'LQD'
      },
      {
        column0: 'Butterfly',
        column1: '57',
        column2: '763',
        column3: '32',
        column4: '320',
        column5: '1357',
        column6: '5305'
      },
      {
        column0: 'Lion',
        column1: '23',
        column2: '426',
        column3: '501',
        column4: '439',
        column5: '5162',
        column6: '1052'
      },
      {
        column0: 'Phoenix',
        column1: '56',
        column2: '845',
        column3: '405',
        column4: '615',
        column5: '8614',
        column6: '5006'
      }
    ]);
  });
});
