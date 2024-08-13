from openpyxl import load_workbook
from win32com import client
import os
import time
from io import BytesIO
import tempfile
from openpyxl.utils.exceptions import InvalidFileException
from src.endpoints.response_json import get_exception_response
def ConvertToPdf(**kwargs):

    try:
        start_time = time.time()
        wb = kwargs['Wb']
        temp = BytesIO()
        wb.save(temp)
        # wb.close()
        temp.seek(0)
        try:

            with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as TempFile:
                excel_file = TempFile.write(temp.getvalue())
            workbook= load_workbook(TempFile.name)
            Wb=workbook.active
            excel = client.Dispatch("Excel.Application")

        except InvalidFileException as e:
            print(f'File Error : {str(e)}')


        image_width, image_height, image_top = kwargs.get('width', 40), kwargs.get('height', 17), kwargs.get('top', 20)
       
        margin_left, margin_right, margin_top, margin_bottom = kwargs.get('margin_left', 0.14), kwargs.get('margin_right', 0.14), kwargs.get('margin_top', 0.3), kwargs.get('margin_top', 0)

        excel.DisplayAlerts = False
        sheets = excel.Workbooks.Open(TempFile.name)
        work_sheets = sheets.ActiveSheet
        column_count = work_sheets.UsedRange.Columns.Count
        print(work_sheets.Cells.Font.Size)
        work_sheets.Cells.WrapText = True
        if  column_count > 8:
            work_sheets.PageSetup.Orientation = 2
            font_size =  8
            width_points =  841.8897637795277
            height_points = 595.2755905511812
        else :
            work_sheets.PageSetup.Orientation = 1
            font_size =  10
            width_points =  595.2755905511812
            height_points = 841.8897637795277

        work_sheets.Cells.Font.Size = font_size
     
        work_sheets.PageSetup.LeftMargin = excel.Application.InchesToPoints(margin_left)
        work_sheets.PageSetup.RightMargin = excel.Application.InchesToPoints(margin_right)
        work_sheets.PageSetup.TopMargin = excel.Application.InchesToPoints(margin_top)
        work_sheets.PageSetup.BottomMargin = excel.Application.InchesToPoints(margin_bottom)
                
        column_width = (width_points - ((margin_left + margin_right ) * 72)) /column_count
        column_width = column_width / 7
        for col in range(1, column_count + 1):
            work_sheets.Columns(col).ColumnWidth = column_width
        
        # page_width = 11.69  
        # column_count = work_sheets.UsedRange.Columns.Count
        # column_width_in_points = excel.Application.InchesToPoints((page_width - 2 * (margin_left + margin_right)) / column_count)
        
        [setattr(shape, 'Width', image_width) or setattr(shape, 'Height', image_height) or setattr(shape, 'Top', image_top ) for shape in work_sheets.Shapes]
        #     # shape.Width=cell_range.Width/2
        #     # shape.Height=cell_range.Height/2


        merged_range={mrng.coord for mrng in Wb.merged_cells.ranges }
        used_range_rows=work_sheets.UsedRange.Rows.Count

        for row in range(1, used_range_rows + 1):
            merged = False
            for mergedrange in merged_range:
                if row >= work_sheets.Range(mergedrange).Row and row <= work_sheets.Range(mergedrange).Row + work_sheets.Range(mergedrange).Rows.Count - 1:
                    merged = True
                    break
            if not merged:
                work_sheets.Rows(row).AutoFit()    
        work_sheets.ExportAsFixedFormat(0, kwargs['pdf_path'])
        sheets.Close(SaveChanges=False)
        # pdf_wb.close(False)
        excel.Quit()
        end_time=time.time()
        workbook.close()
        os.remove(TempFile.name)
        print(f"----used time {end_time-start_time}")
            
        return {'status': 'success'}
    except Exception as e:
        excel.Quit()
       
        print('---------', str(e), get_exception_response(e))
        return get_exception_response(e)