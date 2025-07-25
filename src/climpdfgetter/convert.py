import json
import re
import signal
from pathlib import Path

import chardet
import click
from bs4 import BeautifulSoup
from marker.config.parser import ConfigParser
from marker.converters.pdf import PdfConverter
from marker.converters.table import TableConverter
from marker.models import create_model_dict
from marker.output import text_from_rendered
import openparse
from openparse import processing, Pdf
from PIL import Image
from rich.progress import Progress, SpinnerColumn, TimeElapsedColumn

from .schema import ParsedDocumentSchema
from .utils import _clean_subsections, _collect_from_path, clean_header


def timeout_handler(signum, frame):
    raise TimeoutError()


signal.signal(signal.SIGALRM, timeout_handler)


def _convert_images_to_pdf(files: list, progress):
    progress.log("* Found " + str(len(files_to_convert_to_pdf)) + " files that must first be converted to PDF.")

    success_count = 0
    fail_count = 0

    task1 = progress.add_task("[green]Converting to PDF", total=len(files_to_convert_to_pdf))

    for i in files_to_convert_to_pdf:
        try:
            Image.open(i).save(i.with_suffix(".pdf"), "PDF", save_all=True, resolution=100)
            collected_input_files.append(i.with_suffix(".pdf"))
            success_count += 1
        except ValueError:
            fail_count += 1
        progress.update(task1, advance=1)

    progress.log("\n* Conversion of files to PDF:")
    progress.log("* Successes: " + str(success_count))
    progress.log("* Failures: " + str(fail_count))


def _get_images_from_marker(input_file: Path, output_file: Path):
    config = {
        "output_dir": output_file.parent / output_file.stem,
        "disable_links": False,
        "force_ocr": False,
    }

    config_parser = ConfigParser(config)

    converter = PdfConverter(
        config=config_parser.generate_config_dict(),
        artifact_dict=create_model_dict(),
    )

    rendered = converter(str(input_file))
    _, _2, images = text_from_rendered(rendered)  # TODO, images only?
    return images


def _get_tables_from_marker(input_file: Path, output_file: Path):
    config = {
        "output_dir": output_file.parent / output_file.stem,
        "disable_image_extraction": True,
        "extract_images": False,
        "recognition_batch_size": 4,
        "detection_batch_size": 4,
        "disable_multiprocessing": True,
    }

    config_parser = ConfigParser(config)

    converter = TableConverter(
        config=config_parser.generate_config_dict(),
        artifact_dict=create_model_dict(),
    )

    rendered = converter(str(input_file))
    table_text, _, _2 = text_from_rendered(rendered)
    return table_text


def _get_text_from_openparse(input_file: Path, output_file: Path):
    parser = openparse.DocumentParser(
        use_markitdown=True,
    )
    doc = Pdf(file=input_file)
    parsed_doc = parser.parse(input_file, ocr=False, parse_elements={"images": False, "tables": False, "forms": True, "text": True})
    text = []
    for node in parsed_doc.nodes:
        if node.variant == {'text'}:
            text.append(node._repr_markdown_())
    text = "\n".join(text)
    return text


# def _get_text_tables_from_openparse(input_file: Path, output_file: Path):
#     parser = openparse.DocumentParser(
#         use_markitdown=True,
#         table_args={
#             "parsing_algorithm": "table-transformers",
#             "min_table_confidence": 0.8,
#             "table_output_format": "markdown",
#         }
#     )
#     doc = Pdf(file=input_file)
#     parsed_doc = parser.parse(input_file, ocr=True)#, parse_elements={"images": True, "tables": True, "forms": True, "text": True})
#     text = []
#     for node in parsed_doc.nodes:
#         if node.variant == {'text'}:
#             text.append(node._repr_markdown_())
#         elif node.variant == {'table'}:
#             text.append(node._repr_markdown_())
#     doc.display_with_bboxes(parsed_doc.nodes)
#     import wat; import ipdb; ipdb.set_trace()
#     text = "\n".join(text)
#     return text


def _convert(source: Path, progress, images_flag: bool, tables_flag: bool):
    # import this here since it's a heavy dependency - we don't want to import it if we don't need to

    org = "OSTI"  # TODO: make this configurable
    collected_input_files = _collect_from_path(Path(source))

    files_to_convert_to_pdf = [
        i for i in collected_input_files if i is not None and i.suffix.lower() not in [".pdf", ".json"]
    ]  # skip pdfs, checkpoints, metadata

    if len(files_to_convert_to_pdf):
        _convert_images_to_pdf(files_to_convert_to_pdf, progress)  # done in-place

    success_count = 0
    fail_count = 0

    collected_input_files = [i for i in collected_input_files if i is not None and i.suffix.lower() == ".pdf"]
    progress.log("\n* Found " + str(len(collected_input_files)) + " input PDFs.")

    task2 = progress.add_task("[bright_green]Converting multiple documents to text", total=len(collected_input_files))

    # already-completed output files
    output_dir = Path(str(collected_input_files[0].parent) + "_json")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_files = [i.stem for i in output_dir.iterdir()]

    timeout_json = output_dir / "timeout.json"
    if timeout_json.exists():
        with open(timeout_json, "r") as f:
            timeout_files = json.load(f)
    else:
        timeout_files = []

    no_metadata = False
    try:
        metadata_file = [i for i in Path(source).glob("*metadata.json")][0]
        metadata = json.load(metadata_file.open("r"))
    except IndexError:
        progress.log("No metadata found for " + source + ". Skipping metadata association.")
        no_metadata = True

    if images_flag:
        progress.log("Images: enabled.")
    else:
        progress.log("Images: disabled.")

    if tables_flag:
        progress.log("Tables: enabled.")
    else:
        progress.log("Tables: disabled.")
    for i in collected_input_files:
        signal.alarm(900)

        output_file = output_dir / i.stem
        per_file_dir = output_file.parent / output_file.stem
        if i.stem in output_files or i.stem in timeout_files:  # skip if already converted, or timed out
            success_count += 1
            progress.update(task2, advance=1)
            continue

        try:
            progress.log("Starting conversion for: " + str(i.name))
            if images_flag:
                images = _get_images_from_marker(i, output_file) # DO want amark_images, NOT mark_text
            else:
                images = {}
            if tables_flag:
                table_text = _get_tables_from_marker(i, output_file) # DO want table_text, NOT bmark_images
            else:
                table_text = ""
            raw_text = _get_text_from_openparse(i, output_file)
            # raw_text = _get_text_tables_from_openparse(i, output_file) # DO want text
            if len(images) or len(table_text):
                per_file_dir.mkdir(parents=True, exist_ok=True)

        except TimeoutError:
            progress.log("Timeout while converting: " + str(i.name) + ". Skipping.")
            fail_count += 1
            progress.update(task2, advance=1)
            timeout_files.append(i.stem)
            continue

        except Exception as e:
            progress.log("Error while converting: " + str(i.name) + ". Skipping.")
            fail_count += 1
            progress.update(task2, advance=1)
            continue

        else:
            lines = raw_text.splitlines()

            indexes = []
            headers = []
            text = {}

            for idx, line in enumerate(lines):
                if line.startswith("#") or line.startswith("**"):  # Note we may run into issues if tokens in text start with #, e.g. #TAGs.
                    indexes.append(idx)                            #   ... is there a way of determining if a # is a title or not? maybe need LLM
                    headers.append(line)

            index_pairs = zip(indexes[:-1], indexes[1:])
            for i, (start, end) in enumerate(index_pairs):
                header = headers[i]
                new_header = clean_header(header)
                section = lines[start:end]
                new_section = [i for i in section if i not in [header, "\n", "", []]]
                text[new_header] = "".join(new_section)

            len_subsections = len("".join(text.values()))
            if not len(text) or len_subsections / len(raw_text) < 0.90:  # if no headers, or not enough text has been saved into subsections (for some reason)
                text = {"text": " ".join(lines)}

            output_files.append(output_file)

            if not no_metadata:
                try:
                    matching_metadata = [entry for entry in metadata if output_file.stem == entry["osti_id"]][0]
                    base_text_list = [text]
                    representation = ParsedDocumentSchema(
                        source=org,
                        title=matching_metadata["title"],
                        text=base_text_list,
                        abstract=matching_metadata.get("description", ""),
                        authors=matching_metadata["authors"],
                        publisher=matching_metadata.get("journal_name", ""),
                        date=matching_metadata["publication_date"],
                        unique_id=matching_metadata["osti_id"],
                        doi=matching_metadata.get("doi", ""),
                    )
                    output_rep = representation.model_dump(mode="json")
                except Exception as e:
                    progress.log("Failure while postprocessing " + str(output_file) + ": " + str(e))
                    fail_count += 1
                    continue
            else:
                output_rep = text
            for name, image in images.items():
                image.save(per_file_dir / name)
            with open(output_file.with_suffix(".json"), "w") as f:
                json.dump(text, f)
            if len(table_text):
                with open(per_file_dir / Path("tables.md"), "w") as f:
                    f.write(table_text)
            progress.update(task2, advance=1)
            success_count += 1


    signal.alarm(0)
    with open(timeout_json, "w") as f:
        json.dump(timeout_files, f)

    progress.log("\n* Conversion of PDFs to json:")
    progress.log("* Successes or predetermined-skipped: " + str(success_count))
    progress.log("* Failures: " + str(fail_count))
    progress.log("* Timeout failures: " + str(len(timeout_files)))
    progress.log(
        "Timed out files appended to "
        + str(timeout_json)
        + ". These will be skipped on future conversions."
        + "\nDelete the file if you want to retry them."
    )


@click.command()
@click.argument("source", nargs=1)
@click.option("--images", "-i", is_flag=True)
@click.option("--tables", "-t", is_flag=True)
def convert(source: Path, images: bool, tables: bool):
    """
    Convert PDFs in a given directory ``source`` to json. If the input files are of a different format,
    they'll first be converted to PDF.
    """
    with Progress(SpinnerColumn(), *Progress.get_default_columns(), TimeElapsedColumn()) as progress:
        _convert(source, progress, images, tables)


@click.command()
@click.argument("source", nargs=1)
def epa_ocr_to_json(source: Path):
    """Convert EPA's OCR fulltext to similar json format as output from pdf2json"""

    collected_input_files = _collect_from_path(Path(source))

    click.echo("* Found " + str(len(collected_input_files)) + " input text files.")

    success_count = 0
    fail_count = 0

    collected_input_files = [i for i in collected_input_files if i is not None and i.suffix.lower() == ".txt"]

    click.echo("* Beginning Conversion:")

    for i in collected_input_files:
        signal.alarm(60)
        try:

            with open(i, "rb") as f:
                data = f.read()

            encoding = chardet.detect(data)["encoding"]
            full_text = data.decode(encoding)

            pubnumber = re.findall("<pubnumber>(.*?)</pubnumber>", full_text)[0]
            title = re.findall("<title>(.*?)</title>", full_text)[0]
            year = int(re.findall("<pubyear>(.*?)</pubyear>", full_text)[0])
            authors = re.findall("<author>(.*?)</author>", full_text)
            abstract = re.findall("<abstract>(.*?)</abstract>", full_text)[0]
            origin_format = re.findall("<origin>(.*?)</origin>", full_text)[0]
            publisher = re.findall("<publisher>(.*?)</publisher>", full_text)[0]

            ocr_soup = BeautifulSoup(full_text, "html.parser")
            text = ocr_soup.getText()

            sub_sections = text.split("\n\n\n")
            cleaned_subsections = _clean_subsections(sub_sections)
            # remove pubnumber from first section
            cleaned_subsections[0] = cleaned_subsections[0].replace(pubnumber, "")

            representation = ParsedDocumentSchema(
                source="EPA",
                title=title,
                text=cleaned_subsections,
                abstract=abstract,
                authors=authors,
                origin_format=origin_format,
                publisher=publisher,
                unique_id=pubnumber,
                year=year,
            )

            output_dir = Path(str(i.parent) + "_json")
            output_file = Path(output_dir / i.stem).with_suffix(".json")
            output_file.parent.mkdir(parents=True, exist_ok=True)
            with open(output_file, "w") as f:
                json.dump(representation.model_dump(mode="json"), f)
            success_count += 1

        except KeyboardInterrupt:
            click.echo("Skipping current document: " + str(i))
            fail_count += 1
            continue
        except TimeoutError:
            click.echo("Timeout while converting: " + str(i) + ". Skipping.")
            fail_count += 1
            continue
        except Exception as e:
            click.echo("Failure while converting: " + str(i) + ": " + str(e))
            fail_count += 1
            continue
        finally:
            signal.alarm(0)

    click.echo("* Conversion of EPA OCR text to json:")
    click.echo("* Successes: " + str(success_count))
    click.echo("* Failures: " + str(fail_count))
