from zipfile import ZipFile

def get_manifest_zip_path(zipfile_path):
    with ZipFile(zipfile_path) as z:
        manifest_zip_path = None
        for p in z.namelist():
            if p.endswith('manifest.safe'):
                manifest_zip_path = p
                break
    return "/vsizip/" + zipfile_path.rstrip('/') + '/' + manifest_zip_path

def get_granules_zip_path(zipfile_path):
    zipfile_path.stripped = zipfile_path.rstrip('/')
    with ZipFile(zipfile_path) as z:
        granules_zip_path = []
        for p in z.namelist():
            if p.endswith('.tiff') or p.endswith('.TIFF'):
                granules_zip_path.append("/vsizip/" + zipfile_path + '/' + p)
    return granules_zip_path


def extract_file_from_zip(endswith_str, zipfile_path, out_dir=None):
  with ZipFile(zipfile_path) as z:

    file_zip_path = None
    for p in z.namelist():
        if p.endswith(endswith_str):
            file_zip_path = p
            break
    if file_zip_path is None:
      return None
    # extract it
    if out_dir is None:
      targetpath = z.extract(file_zip_path)
    else:
      targetpath = z.extract(file_zip_path, path=out_dir)
    return targetpath

def extract_manifest_from_zip(zipfile_path, out_dir=None):
  with ZipFile(zipfile_path) as z:
    # look for manifest
    manifest_zip_path = None
    for p in z.namelist():
        if p.endswith('manifest.safe'):
            manifest_zip_path = p
            break
    if manifest_zip_path is None:
      return None
    # extract it
    if out_dir is None:
      targetpath = z.extract(manifest_zip_path)
    else:
      targetpath = z.extract(manifest_zip_path, path=out_dir)
    return targetpath

def extract_annotations_from_zip(zipfile_path, out_dir=None):
  with ZipFile(zipfile_path) as z:
    # look for annotations
    annotations_zip_paths = []
    for p in z.namelist():
      if p.endswith('.xml'):
        annotations_zip_paths.append(p)
    if len(annotations_zip_paths) == 0:
      return None
    # extract them
    targetpaths = []
    if out_dir is None:
      for a in annotations_zip_paths:
        targetpaths.append(z.extract(a))
    else:
      for a in annotations_zip_paths:
        targetpaths.append(z.extract(a, path=out_dir))
    return targetpaths


